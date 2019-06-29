from requests import Request, Session
from client import ElsClient, ElsSearch, ElsAuthor, AbsDoc
from models import *
import pprint as pp
import peewee as pw
import logging
import timeit
import pandas as pd
from colorama import Style
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

# req = Session()
# req.get('https://www.scopus.com/author/highchart.uri')
# print(Colour.UNDERLINE + 'Initial request to SCOPUS ' + Colour.END)


def get_author_citations():
    authors_list = Author.select(Author.id).where(Author.citations.is_null(True)).order_by(Author.cited_by_count.desc())

    for author in authors_list:
        print(Colour.BOLD + 'Request for "' + str(author.id) + '"' + Colour.END)

        params = {
            'authorId': author.id
        }
        citations = req.get('https://www.scopus.com/author/highchart.uri', params=params)
        # Handle empty response
        if citations.text == '':
            obj = {}
            nr_of_years = 0
        else:
            obj = citations.json()
            data = obj.get('citeObj') or obj.get('docObj')
            nr_of_years = str(len(data))
        
        # Store fetched citations
        saved = author.update(citations=obj).where(Author.id == author.id)
        saved.execute()

        print(Colour.OKGREEN + 'Got ' + str(nr_of_years) + ' years of citations for "' + str(author.id) + '"' + Colour.END)
        print(' --- in ' + str(citations.elapsed.total_seconds()) + 's')

def fix_malformed_date():
    invalid_date = Collaboration.select().where(pw.fn.DATE(Collaboration.published).is_null())

    for row in invalid_date:
        article = AbsDoc(scp_id=row.abs_id)
        article.read(client)

        date = article.data['coredata'].get('prism:coverDate')

        saved = row.update(published=date).where(Collaboration.abs_id == row.abs_id)
        saved.execute()
        
        print(Colour.OKGREEN + 'Fixed date for ' + str(row.abs_id) + ', from ' + str(row.published) + ' to ' + str(date) + Colour.END)


def get_article_data(id):
    payload = {
        'field': 'authors'
    }
    p = Request('GET', 'https://api.elsevier.com/content/abstract/scopus_id/' + str(id), params=payload).prepare()
    article = AbsDoc(uri=p.url)

    if article.read(client):
        return article.data
    else:
        return {}

def fullfil_collaboration_authors():
    print('Find malformed collaboration in db...')
    collaborations = Collaboration.select(Collaboration.abs_id).where(Collaboration.authors.is_null(True))
    print('Found some malformed data')
    print('Starting process of fixing, one by one...')

    for row in collaborations:
        try:
            print(str(row.abs_id) + '...', end="")
            article = get_article_data(row.abs_id)
            if article:
                row.authors = article.get('author')
                row.affiliation = article.get('affiliation')
                row.cited_by = article.get('citedby-count')
                row.published = article.get('prism:coverDate')
                row.keywords = article.get('authkeywords')
                row.message = article.get('message')
                row.save()
                print('saved')
            else:
                print('not found')
        except Exception as e:
            print(e)
            

def update_category():
    saved_phys = Coauthors.select()

    for author in saved_phys:
        db_author = Author.get(Author.id == author.id)
        db_author.cat = ['PHYS']
        db_author.save()

        print(Colour.OKGREEN + str(db_author.id) + " now has cat set to " + ','.join(db_author.cat) + Colour.END)

def get_affiliation_current(data):
    return {
        "affiliation-url": "https://api.elsevier.com/content/affiliation/affiliation_id/60032724", 
        "affiliation-id": "60032724", 
        "affiliation-name": "University Medical Center Utrecht", 
        "affiliation-city": "Utrecht", 
        "affiliation-country": "Netherlands"
    }

def update_missing_authors():
    missing = pd.read_csv('./missing_country_phys.csv', usecols=['auth_id'])
    
    for i,row in missing.iterrows():
        try:
            db_author, created = Author.get_or_create(id=row['auth_id'])
            scopus_author = ElsAuthor(author_id=row['auth_id'])
            if scopus_author.read_metrics(client):
                
                db_author.affiliation_current = scopus_author.data.get('affiliation-current')
                db_author.document_count = scopus_author.data['coredata'].get('document-count')
                db_author.cited_by_count = scopus_author.data['coredata'].get('cited-by-count')
                db_author.coauthors_count = scopus_author.data.get('coauthors_count')
                db_author.save()

                print('.', end="")
            else:
                print('-', end="")
        except Exception as e:
            print(e)


re_abs_id = re.compile('SCOPUS_ID:(.*)')

def save_collaboration(raw, id):
    collaboration, created = Collaboration.get_or_create(abs_id=id)
    author_count = raw.get('author-count', {})
    total_authors = int(author_count.get('@total', 0))

    print(str(id) + ' Doc retrived with ' +  str(len(raw.get('author'))) + ' authors, it has a total of ' + str(total_authors))

    collaboration.cited_by = raw.get('citedby-count')
    collaboration.published = raw.get('prism:coverDate')
    collaboration.keywords = raw.get('authkeywords')
    collaboration.message = raw.get('message')
    collaboration.authors = raw.get('author')

    # If there are less authors in `author` list of article do save it into db
    # Else go back to Scopus and get the whole list of authors
    if total_authors > len(raw.get('author')) and total_authors != len(collaboration.authors):
        print("Getting the rest of them", end="")
        data = get_article_data(id)
        authors = data.get('authors', {})
        authors = authors.get('author', [])

        if len(authors) > 0:
            collaboration.authors = authors
            print(" - Done, got " + str(len(authors)))
        else:
            print(' - Something was not good, got ' + str(len(authors)))
    else:
        print('Document alread saved with ' + str(len(collaboration.authors)))

    collaboration.save()

def get_network(author):
    def save_colls(articles):
        for article in articles:
            abs_id = re_abs_id.findall(article.get('dc:identifier'))
            abs_id = abs_id[0]
            save_collaboration(article, abs_id)
    
    # get coauthors articles
    def get_articles(ath):
        print('Getting documents for: "' + str(ath.id) + '"')

        payload = {
            'query':    'AU-ID({0})'.format(ath.id),
            'count':    '100',
            'cursor':    '*',
            'field':   'author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords,message',
            'date':     '2007-2019'
        }
        p = Request('GET', 'https://api.elsevier.com/content/search/scopus', params=payload).prepare()
        search = ElsSearch(p.url)
        search.execute(client)

        print('Author has written: ' + str(search.tot_num_res) + ' documents')
        # Save collaborations
        if (search.tot_num_res > 0):
            save_colls(search.results)
        print(str(search.num_res) + " Documents saved")

        total_saved = 0
        total_saved += search.num_res

        while total_saved < search.tot_num_res:
            payload = {
                'query':    'AU-ID({0})'.format(ath.id),
                'count':    '100',
                'cursor':   search.cursor['@next'],
                'field':   'author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords,message',
                'date':     '2007-2019'
            }
            p = Request('GET', 'https://api.elsevier.com/content/search/scopus', params=payload).prepare()

            search = ElsSearch(p.url)
            search.execute(client)

            # Save collaborations
            save_colls(search.results)
            
            # Store latest fetched page index
            last_page = Author.update(last_page=total_saved).where(Author.id == ath.id)
            last_page.execute()

            # Increment saved
            total_saved += search.num_res
            print(str(total_saved) + " Documents saved")

        author_fetched = Author.update(docs_fetched=True, last_page=total_saved).where(Author.id == ath.id)
        author_fetched.execute()
        print(str(ath.id) + " - DONE" )

    get_articles(author)


def update_missing_articles():
    missing = pd.read_csv('./missing_data_egos.csv', usecols=['ego_id'])
    
    for i,row in missing.iterrows():
        try:
            db_author, created = Author.get_or_create(id=row['ego_id'])
            get_network(db_author)
        except Exception as e:
            print('-')
            print(e)


#get_author_citations()

#fix_malformed_date()

# update_category()

# fullfil_collaboration_authors()

# update_missing_authors()

update_missing_articles()