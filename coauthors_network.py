""" Elsevier data downloader """

import itertools
from requests import Request
from client import ElsClient, ElsSearch
from models import *
from colorama import Style, Fore

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

authors_list = Author.select().where({Author.docs_fetched: False})


@db.atomic()
def save_author(raw):
    try:
        # Check first if author exist
        db_author = Author.get(Author.id == raw['authid'])
    except Author.DoesNotExist:
        # If not save what can be saved
        db_author = Author(id=raw['authid'])
        db_author.full_name = raw['authname']
        aff = raw.get('afid')
        if aff is not None:
            db_author.affiliation_current = aff[0]['$']

        # Write into db
        db_author.save(force_insert=True)
        print(".", end="", flush=True)


# Gather the document list and store each collaboration from authors
# If co author does not exist in db, insert a new record with basic data

for author in authors_list:
    payload = {
        'query':    'AU-ID({0})'.format(author.id),
        'count':    25,
        'field':   'dc:creator,author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords',
        'date':     '2007-2018'
    }

    print(Style.BRIGHT + 'Getting documents for: "' + str(author.id) + '"' + Style.RESET_ALL)

    p = Request('GET', 'https://api.elsevier.com/content/search/scopus', params=payload).prepare()
    search = ElsSearch(p.url)
    search.execute(client)

    print(Colour.OKGREEN + 'Author has written: ' + Colour.BOLD + str(search.tot_num_res) + ' documents' + Colour.END)

    saved_arr = 0

    def save_colls(articles, saved):
        for article in articles:
            if article.get('dc:creator') is not None \
                    and article.get('author') is not None \
                    and len(article['author']) > 1:
                creator = [c for c in article['author'] if c['authname'] == article['dc:creator']][0]
                colls = [c for c in article['author'] if int(c['@seq']) > 1]

                save_author(creator)

                for coauth in colls:
                    # Save both authors
                    save_author(coauth)

                    with db.atomic() as transaction:
                        collaboration = Collaboration()
                        collaboration.author = int(creator['authid'])
                        collaboration.co_author = int(coauth['authid'])
                        collaboration.cited_by = article['citedby-count']
                        collaboration.year = article['prism:coverDate'].split('-')[0]
                        collaboration.keywords = article.get('authkeywords')

                        try:
                            collaboration.save(force_insert=True)
                            saved += 1
                            print(".", end="")
                        except IntegrityError:
                            # Because this block of code is wrapped with "atomic", a
                            # new transaction will begin automatically after the call
                            # to rollback().
                            transaction.rollback()

        return saved

    print(Colour.UNDERLINE + str(search.num_res) + " Documents gathered" + Colour.END)
    # Save
    saved_arr += save_colls(search.results, saved_arr)

    total_saved = 0
    total_saved += search.num_res

    next_url = None
    while total_saved < search.tot_num_res:
        for e in search.links:
            if e['@ref'] == 'next':
                next_url = e['@href']

        search = None
        search = ElsSearch(next_url)
        search.execute(client)

        # Save
        saved_arr += save_colls(search.results, saved_arr)

        # Increment saved
        total_saved += search.num_res

        print(str(saved_arr) + " Collaboration saved")
        print(Colour.UNDERLINE + str(total_saved) + " Documents gathered" + Colour.END)

    fetched = Author.update(docs_fetched=True).where(Author.id == author.id)
    fetched.execute()
    print(Colour.OKGREEN + "DONE" + Colour.END)
