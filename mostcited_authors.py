""" Elsevier author data downloader """
from requests import Request, exceptions
from client import ElsClient, ElsSearch, ElsAuthor
from models import *
from colorama import Style, Fore
import re
import atexit
import time
import pprint as pp
import logging
import boto3

# Create an SNS client
awsclient = boto3.client(
    "sns",
    aws_access_key_id=os.environ.get('AWS_KEY'),
    aws_secret_access_key=os.environ.get('AWS_SECRET'),
    region_name="us-east-1"
)

# Create the topic 
topic = awsclient.create_topic(Name="Iconic_Data")
topic_arn = topic['TopicArn']  # get its Amazon Resource Name


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

# countries = ' OR '.join(['AFFIL({0})'.format(c) for c in countries])

cat = 'SOCI'

# Limit number of articles for every cat and country
# limit = 5187
limit = 1500

# Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

# Initialize elsclient client
client = ElsClient(config['apikey'])

def main():
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
        # Check if coauthors were already saved
        if (created is False and db_coauthor.co_list) and len(db_coauthor.co_list) > 0:
            return db_coauthor.co_list

        coauthors = []
        print(Style.BRIGHT + 'Getting coauthors of: "' + str(ath.id) + '"' + Style.RESET_ALL)

        payload = {
            'co-author':  '%s' % ath.id,
            'count':    25,
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

    def save_authors(authors):
        # Save author
        # get author coauthors
        # save coauthors data
        for author in authors:
            db_author = save_author(author, save_metrics=True)
            # Save categories only for authors and not for coauthors
            cat_tuple_to_list = db_author.cat
            if isinstance(cat_tuple_to_list, tuple):# because fuck python 
                cat_tuple_to_list = cat_tuple_to_list[0]# because fuck python 
            else:
                cat_tuple_to_list = list(db_author.cat or []) # because fuck python 
            cat_tuple_to_list = [i for i in cat_tuple_to_list if not isinstance(i, list)] # because fuck python 

            db_author.cat = list(set(cat_tuple_to_list + [cat]))
            db_author.save()

            coauthors = get_coauthors(db_author)
            # Store only unique coauthors ids
            coauthors = list(set(coauthors))  
            fetched = Coauthors.update(co_list=coauthors).where(Coauthors.id == db_author.id)
            fetched.execute()

    def save_latest(new_country, offset=0, total=0, is_final=False):
        with open("config.json", "w") as io:
            new_config = config
            if is_final:
                new_config[new_country] = 1
                new_config['latest_offset'] = 0
                new_config['total'] = 0
            else:
                new_config['latest_offset'] = offset
                new_config['total'] = total
            
            json.dump(new_config, io)

    for country in countries:
        saved = config.get(country)

        if not saved == 1 and config['latest_offset'] <= config['total']:
            payload = {
                'query': 'SUBJAREA({0}) AND AFFIL({1})'.format(cat, country),
                'count': 50,
                'start': config['latest_offset'],
                'sort': '-document-count',
            }
            print('Getting most cited authors from ' + country + ' starting from latest ' + str(config['latest_offset']))

            p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()
            search = ElsSearch(p.url)
            search.execute(client)

            print(Colour.BOLD + "Going to save " + str(search.num_res) + " authors " + Colour.END)

            # Save
            save_authors(search.results)

            total_saved = config['latest_offset']
            total_saved += search.num_res
            save_latest(country, total_saved, limit)

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
                save_latest(country, total_saved, limit)

                print(Colour.BOLD + str(total_saved) + " Authors saved" + Colour.END, end="")

            print(Colour.OKGREEN + "DONE" + Colour.END)
            save_latest(country, 0, 0, is_final=True)

        elif config['latest_offset'] == config['total']:
            save_latest(country, 0, 0, is_final=True)


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
            awsclient.publish(
                PhoneNumber="+40749318307",
                Message="Iconic scraper it's down!",
            )
        else:
            break
