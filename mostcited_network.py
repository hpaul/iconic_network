""" Elsevier data downloader """
import itertools
from requests import Request, exceptions
from client import ElsClient, ElsSearch
from models import *
from colorama import Style, Fore
import re
import atexit
import time
import pprint as pp


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
# calculate the sample
# select 1000 from each country
# SELECT
#     id, (json_extract(full_name, "$.surname") || ' ' || json_extract(full_name, "$.given-name")) AS name,
#     cited_by_count, (json_extract(affiliation_current, "$.affiliation-city") || ', ' || json_extract(affiliation_current, "$.affiliation-country")) AS city,
#     docs_fetched
# FROM
#     author
# WHERE
#     json_extract(affiliation_current, "$.affiliation-country")
#     IN ('Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'United Kingdom')
#     AND cited_by_count IS NOT NULL
# ORDER BY
#     cited_by_count DESC,
#     document_count DESC
# LIMIT 600
authors_list = Author.select().order_by(Author.cited_by_count.desc()).limit(1000)

@db.atomic()
def save_collaboration(raw, id):
    try:
        # Check first if author exist
        coll = Collaboration.get(Collaboration.abs_id == id)
        print(",", end="")
    except Collaboration.DoesNotExist:
        with db.atomic() as transaction:
            collaboration = Collaboration()
            collaboration.abs_id = id
            collaboration.authors = raw.get('author')
            collaboration.affiliation = raw.get('affiliation')
            collaboration.cited_by = raw.get('citedby-count')
            collaboration.published = raw.get('prism:coverDate')
            collaboration.keywords = raw.get('authkeywords')
            collaboration.message = raw.get('message')

            try:
                collaboration.save(force_insert=True)
                print(".", end="")
            except IntegrityError:
                # Because this block of code is wrapped with "atomic", a
                # new transaction will begin automatically after the call
                # to rollback().
                transaction.rollback()


re_abs_id = re.compile('SCOPUS_ID:(.*)')

# Gather the document list and store each collaboration from authors
# If co author does not exist in db, insert a new record with basic data


def main():
    for author in authors_list:
        # get author coauthors
        # save coauthors data
        # get coauthors articles
        try:
            coauthortable_author = Coauthors.get(Coauthors.id == author.id)
        except Coauthors.DoesNotExist:
            coauthortable_author = Coauthors(id=author.id)
            # Write into db
            coauthortable_author.save(force_insert=True)
        
        coauthortable_author = Coauthors.get(Coauthors.id == author.id)

        def get_coauthors(ath):
            
            def save_coauthors(coauthors):
                colist = []
                for coauthor in coauthors:
                    a_id = str(coauthor['dc:identifier']).split('AUTHOR_ID:')
                    a_id = int(a_id[-1])
                    colist.append(a_id)
                    db_author = None
                    # Check if author was already saved in db
                    try:
                        db_author = Author.get(Author.id == a_id)
                    except Author.DoesNotExist:
                        subject_areas = coauthor.get('subject-area') or []
                        if isinstance(coauthor.get('subject-area'), list) is False:
                            subject_areas = [coauthor.get('subject-area')]
                        
                        db_author = Author(id=a_id)
                        db_author.full_name = coauthor['preferred-name']

                        if coauthor.get('subject-area') is not None:
                            db_author.subject_areas = [dict(frequency=s['@frequency'], name=s['$']) for s in subject_areas]
                        db_author.document_count = coauthor.get('document-count')
                        db_author.affiliation_current = coauthor.get('affiliation-current')

                        # Write into db
                        db_author.save(force_insert=True)

                return colist


            db_author = Coauthors.get(Coauthors.id == author.id)
            if db_author.co_list is True and len(db_author.co_list) > 0:
                return db_author.co_list
            
            coauthors = []

            print(Style.BRIGHT + 'Getting coauthors of: "' + str(ath.id) + '"' + Style.RESET_ALL)

            payload = {
                'co-author':  '%s' % ath.id,
                'count':    25,
                'start':    ath.last_page,
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
            
            return coauthors

        def get_articles(ath):

            def save_colls(articles):
                for article in articles:
                    if article.get('author') is not None and len(article['author']) > 1:

                        id = re_abs_id.findall(article.get('dc:identifier'))
                        id = id[0]
                        save_collaboration(article, id)
            

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

            print(Colour.UNDERLINE + str(search.num_res) + " Documents gathered" + Colour.END)
            # Save collaborations
            save_colls(search.results)

            total_saved = 0
            total_saved += search.num_res

            next_url = None
            while total_saved < search.tot_num_res:
                
                # Store latest fetched page index
                last_page = Author.update(last_page=total_saved).where(Author.id == ath.id)
                last_page.execute()

                payload = {
                    'query':    'AU-ID({0})'.format(ath.id),
                    'count':    '150',
                    'cursor':   search.cursor['@next'],
                    'field':   'author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords,message',
                    'date':     '2007-2019'
                }
                p = Request('GET', 'https://api.elsevier.com/content/search/scopus', params=payload).prepare()

                search = ElsSearch(p.url)
                search.execute(client)

                # Save collaborations
                save_colls(search.results)

                # Increment saved
                total_saved += search.num_res

                print(Colour.UNDERLINE + str(total_saved) + " Abstracts gathered" + Colour.END)

            fetched = Author.update(docs_fetched=True, last_page=total_saved).where(Author.id == ath.id)
            fetched.execute()
            print(Colour.OKGREEN + "DONE" + Colour.END)


        if coauthortable_author.saved is False:
            coauthors = get_coauthors(author)
            # Get unique values
            coauthors = list(set(coauthors))

            fetched = Coauthors.update(co_list=coauthors).where(Coauthors.id == author.id)
            fetched.execute()

            print(Colour.OKGREEN + "Getting network for author: " + str(author.id))
            for coauthor in coauthors:
                coath = Author.get(Author.id == coauthor)
                if coath.docs_fetched is False:
                    get_articles(coath)

            fetched = Coauthors.update(saved=True).where(Coauthors.id == author.id)
            fetched.execute()

def exit_handler():
    main()

atexit.register(exit_handler)

if __name__=='__main__':
    for x in range(0, 10):
        try:
            main()
            str_error = None
        except exceptions.HTTPError as err:
            str_error = err
            pp.pprint(err)
            pass
        
        if str_error:
            time.sleep(2)
        else:
            break
