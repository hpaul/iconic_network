""" Elsevier data downloader """

import itertools
from requests import Request
from client import ElsClient, ElsSearch
from models import *
from colorama import Style, Fore
import re
import atexit
import time


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

authors_list = Author.select().where(Author.docs_fetched == False)


@db.atomic()
def save_collaboration(raw, id):
    try:
        # Check first if author exist
        coll = Collaboration.get(Collaboration.abs_id == id)
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
        payload = {
            'query':    'AU-ID({0})'.format(author.id),
            'count':    25,
            'start':    author.last_page,
            'field':   'author-count,author,dc:identifier,prism:coverDate,citedby-count,authkeywords,message,affiliation',
            'date':     '2007-2019'
        }

        def save_colls(articles):
            for article in articles:
                if article.get('author') is not None and len(article['author']) > 1:

                    id = re_abs_id.findall(article.get('dc:identifier'))
                    id = id[0]
                    save_collaboration(article, id)

        print(Style.BRIGHT + 'Getting documents for: "' + str(author.id) + '"' + Style.RESET_ALL)

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
            for e in search.links:
                if e['@ref'] == 'next':
                    next_url = e['@href']

            # Store latest fetched page index
            last_page = Author.update(last_page=total_saved).where(Author.id == author.id)
            last_page.execute()

            search = None
            search = ElsSearch(next_url)
            search.execute(client)

            # Save collaborations
            save_colls(search.results)

            # Increment saved
            total_saved += search.num_res

            print(Colour.UNDERLINE + str(total_saved) + " Abstracts gathered" + Colour.END)

        fetched = Author.update(docs_fetched=True).where(Author.id == author.id)
        fetched.execute()
        print(Colour.OKGREEN + "DONE" + Colour.END)


def exit_handler():
    # let the db save last writes
    time.sleep(1)


atexit.register(exit_handler)

if __name__=='__main__':
    main()
