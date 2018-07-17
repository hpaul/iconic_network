""" Elsevier data downloader """

from requests import Request
from client import ElsClient, ElsSearch, ElsAuthor
from models import *

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


# Limit number of articles for every cat and country
# limit = 5187
limit = 5000

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

for country in countries:
    for subject in cats:
        db.connect()

        payload = {
            'query': 'AFFIL({0}) AND SUBJAREA({1})'.format(country, subject),
            'count': 200
        }

        print('Getting authors in: "{0} {1}"   '.format(country, subject).rjust(10), end="")

        p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
        search = ElsSearch(p.url)
        search.execute(client)

        print(Colour.BOLD + str(search.num_res) + " Authors saved" + Colour.END)

        def save_authors(authors):
            # Save each author in db
            for author in authors:
                a_id = str(author['dc:identifier']).split('AUTHOR_ID:')
                a_id = int(a_id[-1])
                db_author = None
                # Check if author was already saved in db
                try:
                    db_author = Author.get(Author.id == a_id)
                    db_author.country = list(db_author.country)
                    db_author.country.append(country)
                    # Only unique values
                    db_author.country = list(set(db_author.country))

                    db_author.save()
                except Author.DoesNotExist:
                    if isinstance(author['subject-area'], list) is False:
                        author['subject-area'] = [author['subject-area']]

                    db_author = Author(id=a_id)
                    db_author.full_name = author['preferred-name']
                    db_author.cat = [subject]
                    db_author.country = [country]
                    db_author.subject_areas = [dict(frequency=s['@frequency'], name=s['$']) for s in author['subject-area']]
                    db_author.document_count = author.get('document-count')
                    db_author.affiliation_current = author['affiliation-current']

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
