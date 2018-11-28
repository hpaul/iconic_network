""" Elsevier data downloader """

from requests import Request
from client import ElsClient, ElsSearch, ElsAuthor
from models import *
import pprint as pp

class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


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

countries = ' OR '.join(['AFFIL({0})'.format(c) for c in countries])

cats = [
    'SOCI'
]

# Limit number of articles for every cat and country
# limit = 5187
limit = 5000

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

for subject in cats:
    db.connect()

    payload = {
        'query': 'SUBJAREA({0}) AND {1}'.format(subject, countries),
        'count': 50,
        'start': 0,
        'sort': '-document-count',
    }

    print('Getting most cited authors:')

    p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
    search = ElsSearch(p.url)
    search.execute(client)

    print(Colour.BOLD + "Going to save " + str(search.num_res) + " authors " + Colour.END)

    def save_authors(authors):
        # Save each author in db
        for author in authors:
            a_id = str(author['dc:identifier']).split('AUTHOR_ID:')
            a_id = int(a_id[-1])

            # Request metrics for author profile
            metrics = ElsAuthor(author_id=a_id)
            metrics.read_metrics(client)

            db_author = None
            # Check if author was already saved in db
            try:
                db_author = Author.get(Author.id == a_id)
                db_author.document_count = metrics.data['coredata'].get('document-count')
                db_author.cited_by_count = metrics.data['coredata'].get('cited-by-count')
                db_author.h_index = metrics.data.get('h_index')
                db_author.coauthors_count = metrics.data.get('coauthors_count')
                db_author.is_sample = True

                db_author.save()
            except Author.DoesNotExist:
                if isinstance(author['subject-area'], list) is False:
                    author['subject-area'] = [author['subject-area']]

                db_author = Author(id=a_id)
                db_author.full_name = author['preferred-name']
                db_author.cat = [subject]
                db_author.subject_areas = [dict(frequency=s['@frequency'], name=s['$']) for s in author['subject-area']]
                db_author.affiliation_current = author['affiliation-current']
                
                db_author.document_count = metrics.data['coredata'].get('document-count')
                db_author.cited_by_count = metrics.data['coredata'].get('cited-by-count')
                db_author.h_index = metrics.data.get('h_index')
                db_author.coauthors_count = metrics.data.get('coauthors_count')
                db_author.is_sample = True

                # Write into db
                db_author.save(force_insert=True)
                print(".", end="", flush=True)

    # Save
    save_authors(search.results)

    total_saved = 0
    total_saved += search.num_res

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

        print(Colour.BOLD + str(total_saved) + " Authors saved" + Colour.END, end="")

    db.close()
    print()
    print(Colour.OKGREEN + "DONE" + Colour.END)

