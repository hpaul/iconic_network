""" Elsevier data downloader """

from requests import Request
from client import ElsClient, ElsSearch, ElsAuthor
from playhouse.sqlite_ext import *
import json
from peewee import *

# The initialisation
db = SqliteDatabase('./iconic.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': 10000,  # 10000 pages, or ~40MB
    'foreign_keys': 1,  # Enforce foreign-key constraints
})


class BaseModel(Model):
    class Meta:
        database = db


class Author(BaseModel):
    id = BigIntegerField(unique=True, index=True, primary_key=True)
    full_name = TextField(index=True)
    subject_areas = JSONField()
    document_count = BigIntegerField()
    cited_by_count = BigIntegerField()
    citations_count = BigIntegerField()
    h_index = BigIntegerField()
    coauthors_count = BigIntegerField()
    # Data about affiliation
    affiliation_current = JSONField()
    cat = TextField()
    country = TextField()


db.connect()
db.create_tables([Author])
db.close()


class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


countries = [
    'Netherlands',
    'Spain',
    'United Kingdom',
    'Germany',
    'France',
    'Romania',
    'Slovenia',
    'Sweden',
    'Poland',
    'Italy'
]

cats = [
    'PHYS'
]

# Initialize database

# Limit number of articles for every cat and country
# limit = 5187
limit = 10

# We only need publications between 2008 and 2018
# years = 'PUBYEAR > 2007 AND PUBYEAR < 2019'

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

# Open file to writ


for country in countries:
    for subject in cats:
        db.connect()
        total_saved = 0

        payload = {
            'query': 'AFFIL({0}) AND SUBJAREA({1})'.format(country, subject)
        }

        print('Getting authors in: "{0} {1}"   '.format(country, subject).rjust(10), end="")

        p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
        search = ElsSearch(p.url)
        search.execute(client)

        print(Colour.BOLD + str(search.num_res) + " Authors loaded" + Colour.END)

        def save_authors(authors):
            # Save each author in db
            for author in authors:
                a_id = str(author['dc:identifier']).split('AUTHOR_ID:')
                a_id = int(a_id[-1])
                db_author = None
                # Check if author was already saved in db
                try:
                    db_author = Author.get(Author.id == a_id)
                except Author.DoesNotExist:
                    a_name = author['preferred-name']['surname'] + ' ' + author['preferred-name']['given-name']
                    db_author = Author(id=a_id)
                    db_author.cat = subject
                    db_author.country = country
                    db_author.full_name = a_name
                    db_author.subject_areas = [dict(frequency=s['@frequency'], name=s['$']) for s in author['subject-area']]
                    db_author.document_count = author['document-count']
                    db_author.affiliation_current = author['affiliation-current']

                    a_req = ElsAuthor(uri=author['prism:url'])
                    # Read author data, then write to disk
                    if a_req.read_metrics(client):
                        db_author.cited_by_count = a_req.data['coredata']['cited-by-count']
                        db_author.citations_count = a_req.data['coredata']['citation-count']
                        db_author.h_index = a_req.data['h-index']
                        db_author.coauthors_count = a_req.data['coauthor-count']

                    # Write into db
                    db_author.save()
                    print(".", end="", flush=True)

        # Save
        save_authors(search.results)

        total_saved += search.num_res
        # limit = search.tot_num_res

        next_url = None
        while total_saved < limit:
            for e in search.links:
                if e['@ref'] == 'next':
                    next_url = e['@href']

            search = None
            search = ElsSearch(next_url)
            search.execute(client)

            # Save
            save_authors(search.results)

            # Increment saved
            total_saved += search.num_res

        db.close()
        print(Colour.OKGREEN + "DONE" + Colour.END)
