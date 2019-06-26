""" Elsevier data downloader """
import itertools
from requests import Request, exceptions
from client import ElsClient, ElsSearch, ElsAuthor
from models import *
from colorama import Style
import re
import sys
from retrying import retry
import logging
# logger = logging.getLogger('peewee')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

countries = [
    'Austria',
    'Belgium',
    'Bulgaria',
    'Croatia',
    'Cyprus',
    'Czech Republic',
    'Denmark',
    'Estonia',
    'Finland',
    'France',
    'Germany',
    'Greece',
    'Hungary',
    'Ireland',
    'Italy',
    'Latvia',
    'Lithuania',
    'Luxembourg',
    'Malta',
    'Netherlands',
    'Poland',
    'Portugal',
    'Romania',
    'Slovakia',
    'Slovenia',
    'Spain',
    'Sweden',
    'United Kingdom'
]

class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


# We only need publications between 2007 and 2018
years = 'PUBYEAR > 2006 AND PUBYEAR < 2019'

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

query = """
SELECT
    id,
    json_extract(affiliation_current, '$.affiliation-country') AS country_origin
FROM
    author
WHERE
    cat LIKE '%{}%'
    AND country_origin IN ({})
ORDER BY
    cited_by_count DESC
LIMIT 1200
"""

query = query.format('PHYS', ', '.join(["'{}'".format(c) for c in countries]))
authors_list = db.execute_sql(query)


# Return Author peewee model
def save_author(author, save_metrics=False):
    a_id = str(author['dc:identifier']).split('AUTHOR_ID:')
    a_id = int(a_id[-1])

    if isinstance(author.get('subject-area'), list) is False:
        author['subject-area'] = [author.get('subject-area')]

    # Check if author was already saved in db
    db_author, created = Author.get_or_create(id=a_id)

    if created:
        db_author.full_name=author.get('preferred-name'),
        db_author.subject_areas=[dict(frequency=s['@frequency'], name=s['$']) for s in author['subject-area'] if s is not None],
        db_author.affiliation_current=author.get('affiliation-current')

        if save_metrics:
            # Request metrics for author profile
            metrics = ElsAuthor(author_id=a_id)
            metrics.read_metrics(client)
            db_author.document_count = metrics.data['coredata'].get('document-count')
            db_author.cited_by_count = metrics.data['coredata'].get('cited-by-count')
            db_author.h_index = metrics.data.get('h_index')
            db_author.coauthors_count = metrics.data.get('coauthors_count')
            db_author.is_sample = True

        db_author.save()

        print("+1 ", end="", flush=True)
    else:
        print(".", end="", flush=True)

    return db_author

def get_coauthors(ath):   
    def save_coauthors(coauthors):
        colist = []
        for coauthor in coauthors:
            db_author = save_author(coauthor)
            
            colist.append(db_author.id)
        return colist

    # Check for author row in coauthors table
    db_coauthor, created = Coauthors.get_or_create(id=ath.id)

    coauthors = []
    print(Style.BRIGHT + 'Getting coauthors of: "' + str(ath.id) + '"' + Style.RESET_ALL)

    payload = {
        'co-author':  '%s' % ath.id,
        'count':    50,
        'start':    db_coauthor.last_page or 0,
    }
    p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
    search = ElsSearch(p.url)
    search.execute(client)
    print(Colour.OKGREEN + 'Author has ' + Colour.BOLD + str(search.tot_num_res) + ' coauthors' + Colour.END)

    coauthors = coauthors + save_coauthors(search.results)
    total_saved = 0
    total_saved += search.num_res

    while total_saved < search.tot_num_res:
        # Store latest fetched page index
        last_page = Coauthors.update(last_page=total_saved).where(Coauthors.id == ath.id)
        last_page.execute()

        search = None
        # Build next url
        payload = {
            'co-author':  '%s' % ath.id,
            'count':    25,
            'start':    total_saved,
        }
        p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
        search = ElsSearch(p.url)
        search.execute(client)

        # Save authors
        coauthors = coauthors + save_coauthors(search.results)

        # Increment saved
        total_saved += search.num_res

    print(Colour.BOLD + str(total_saved) + " Coauthors saved for '" + str(ath.id) + "'" + Colour.END,)

    return coauthors


def save_collaboration(raw, id):
    collaboration, created = Collaboration.get_or_create(abs_id=id)
    author_count = raw.get('author-count', {})
    total_authors = int(author_count.get('@total', 0))

    print('Doc retrived with ' +  str(len(raw.get('author'))) + ' authors, it has a total of ' + str(total_authors))

    collaboration.cited_by = raw.get('citedby-count')
    collaboration.published = raw.get('prism:coverDate')
    collaboration.keywords = raw.get('authkeywords')
    collaboration.message = raw.get('message')
    collaboration.authors = raw.get('author')

    # If there are less authors in `author` list of article do save it into db
    # Else go back to Scopus and get the whole list of authors
    if total_authors > len(raw.get('author')):
        print("Getting the rest of them", end="")
        data = get_article_data(id)
        authors = data.get('authors', {})
        authors = authors.get('author', [])

        if len(authors) > 0:
            collaboration.authors = authors
            print(" - Done, got " + str(len(authors)))
        else:
            print(' - Something was not good, got ' + str(len(authors)))

    collaboration.save()

re_abs_id = re.compile('SCOPUS_ID:(.*)')


# Gather the document list and store each collaboration from authors
# If co author does not exist in db, insert a new record with basic data
# Wait for 2 seconds between every retries (max 5)
@retry(stop_max_attempt_number=5, wait_fixed=2000)
def get_network(coauthortable_author):
    def save_colls(articles):
        for article in articles:
            abs_id = re_abs_id.findall(article.get('dc:identifier'))
            abs_id = abs_id[0]
            save_collaboration(article, abs_id)
    
    # get coauthors articles
    def get_articles(ath):
        print(Style.BRIGHT + 'Getting documents for: "' + str(ath.id) + '"' + Style.RESET_ALL)

        payload = {
            'query':    'AU-ID({0})'.format(ath.id),
            'count':    '150',
            'cursor':    '*',
            'field':   'author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords,message',
            'date':     '2007-2019'
        }
        p = Request('GET', 'https://api.elsevier.com/content/search/scopus', params=payload).prepare()
        search = ElsSearch(p.url)
        search.execute(client)

        print(Colour.OKGREEN + 'Author has written: ' + Colour.BOLD + str(search.tot_num_res) + ' documents' + Colour.END)
        # Save collaborations
        if (search.tot_num_res > 0):
            save_colls(search.results)
        print(Colour.UNDERLINE + str(search.num_res) + " Documents saved" + Colour.END)

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
            print(Colour.UNDERLINE + str(total_saved) + " Documents saved" + Colour.END)

        author_fetched = Author.update(docs_fetched=True, last_page=total_saved).where(Author.id == ath.id)
        author_fetched.execute()
        print(Colour.OKGREEN + str(ath.id) + " - DONE" + Colour.END)

    if coauthortable_author.saved is False:
        coauthors = coauthortable_author.co_list

        print(Colour.OKGREEN + "Getting network for author: " + str(coauthortable_author.id))
        for coauthor in coauthors:
            coath = Author.get(Author.id == coauthor)
            if coath.docs_fetched is False:
                get_articles(coath)

        fetched = Coauthors.update(saved=True).where(Coauthors.id == coauthortable_author.id)
        fetched.execute()
        print(Colour.OKGREEN + str(coauthortable_author.id) + " - DONE" + Colour.END)
    else:
        print(Colour.OKGREEN + "Network of author: " + str(coauthortable_author.id) + " was saved before!" + Colour.END)


# Some authors got problems with list of author
# So it should be refetched
def update_co_list(coauthor, db_author):
    if len(coauthor.co_list) < 90:
        print('We shall update the coauthor list for ' + str(coauthor.id))
        coauthors = get_coauthors(db_author)
        # Store only unique coauthors ids
        coauthors = list(set(coauthors))  
        fetched = Coauthors.update(co_list=coauthors, saved=False).where(Coauthors.id == db_author.id)
        fetched.execute()


def main():
    for author in authors_list:
        db_author = Author.get(Author.id == author[0])
        coauthortable_author = Coauthors.get(Coauthors.id == author[0])
        
        update_co_list(coauthortable_author, db_author)
        get_network(coauthortable_author)


if __name__=='__main__':
    try:
        main()
    except exceptions.HTTPError as err:
        print(err)
        sys.exit('Something bad happened! Check ^')
