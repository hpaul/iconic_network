from requests import Request, Session
from client import ElsClient, ElsSearch, ElsAuthor
from models import *
import pprint as pp

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

req = Session()
req.get('https://www.scopus.com/author/highchart.uri')

print(Colour.UNDERLINE + 'Initial request to SCOPUS ' + Colour.END)


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
            nr_of_years = str(len(obj['citeObj']))
        
        # Store fetched citations
        saved = author.update(citations=obj).where(Author.id == author.id)
        saved.execute()


        print(Colour.OKGREEN + 'Got ' + nr_of_years + ' years of citations for "' + str(author.id) + '"' + Colour.END)
        print(' --- in ' + str(citations.elapsed.total_seconds()) + 's')

get_author_citations()