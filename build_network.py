from models import *
import itertools
import csv
import atexit
import traceback
import logging
import os.path
import time
import pprint as pp
from sshtunnel import SSHTunnelForwarder
import pymongo
from concurrent.futures import ThreadPoolExecutor
from functools import partial
# logger = logging.getLogger('peewee')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())

MONGO_HOST = "54.221.190.67"
MONGO_DB = "iconic"
SSH_USER = "ubuntu"

server = SSHTunnelForwarder(
    MONGO_HOST,
    ssh_username=SSH_USER,
    ssh_pkey="/Users/hpaul/.ssh/id_rsa",
    remote_bind_address=('127.0.0.1', 27017)
)
server.start()

mongo = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
iconic_db = mongo.iconic

class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def find_citations(author):
    articles = iconic_db.network.find({
        '$or': [
            { 'source': author.id },
            { 'target': author.id }
        ]
    }, { 'articles': 1, '_id': 0 })

    abs_ids = []
    for conn in articles:
        ids = (str(conn['articles'])).split(',')
        abs_ids = abs_ids + ids

    abs_ids = list(set(abs_ids)) # get unique
    citations = Collaboration.select(fn.sum(Collaboration.cited_by).alias('total')).where(Collaboration.abs_id.in_(abs_ids))

    # pp.pprint('Found {} citations for {}'.format(str(citations[0].total), str(author.id)))
    # author.cited_by_count = citations[0].total
    # author.save()
    return citations[0].total

def build_node_row(author):
    name = country = city = university = ''
    name = '{} {}'.format(author.full_name.get('surname'), author.full_name.get('given-name'))
    citations = author.cited_by_count or find_citations(author)
    if author.affiliation_current:
        country = author.affiliation_current.get('affiliation-country')
        city = author.affiliation_current.get('affiliation-city')
        university = author.affiliation_current.get('affiliation-name')
    
    return (author.id,name,country,city,university,citations)

def aggregate_rows(conns):
    def agg_articles(arr):
        return (str(arr)).split(',')
    
    groups = []

    for key, group in itertools.groupby(conns, key=lambda i: '{}-{}-{}'.format(i['source'],i['target'],i['year'])):
        groups.append(list(group))

    rows = []
    for group in groups:
        articles = [agg_articles(a['articles']) for a in group]
        articles = sum(articles, [])

        row = {
            'source': group[0]['source'],
            'target': group[0]['target'],
            'year': group[0]['year'],
            'weight': len(group),
            'articles': articles
        }
        rows.append(row)

    return rows

def build_edge_row(conn):
    # find keywords of each connection
    articles = Collaboration.select(Collaboration.keywords).where(Collaboration.abs_id.in_(conn['articles']))
    
    keywords = []
    for article in articles:
        keys = article.keywords or ''
        keys = keys.split(' | ')
        keywords = keywords + [i.strip() for i in keys]
        
    return [conn['source'], conn['target'], conn['year'], conn['weight'], ','.join(keywords)]
    

def build_network(author):
    try:
        author_id, co_list, country = author
        co_list = json.loads(co_list)
    
        print(Colour.BOLD + "Build network for: " + str(author_id) + Colour.END)

        nodes = "data/%s_nodes.csv" % author_id
        edges = "data/%s_edges.csv" % author_id

        if os.path.exists(nodes):
            os.remove(nodes)
        if os.path.exists(edges):
            os.remove(edges)

        with open(nodes, "wt") as nodes, open(edges, "wt") as edges:
            # CSV with nodes and columns
            nodes.truncate(0)
            author_nodes = csv.writer(nodes, 'excel')
            author_nodes.writerow(('Id', 'Name', 'Country', 'City', 'University', 'Citations'))
            
            authors = Author.select().where(Author.id.in_(co_list))
            for iauthor in authors:
                author_nodes.writerow(build_node_row(iauthor))

            pp.pprint('Building nodes connections for ' + str(author_id) + '...')
            # CSV with edges columns
            edges.truncate(0)
            author_edges = csv.writer(edges, 'excel')
            author_edges.writerow(['Source', 'Target', 'Year', 'Weight', 'Keywords'])
            
            conns = iconic_db.network.find({
                '$and': [
                    {'source': { '$in': co_list } },
                    {'target': { '$in': co_list } }
                ]
            }, { '_id': 0 })

            conns = aggregate_rows(conns)
            for edge in conns:
                author_edges.writerow(build_edge_row(edge))
        
        print('DONE - ' + str(author_id))
        return True
    except Exception as e:
        logging.error(traceback.format_exc())
        return False

if __name__=='__main__':
    # Get list of most cited authors and their co-authors
    # For each co-author get his documents
    # Go in each document and build the network
    # Select author co-authors and exclude others
    # Save nodes in *id*_nodes.csv
    # Save edges in *id*_edges.csv
    authors = Coauthors.select(Coauthors.id, Coauthors.co_list, Author.cited_by_count) \
                       .join(Author, on=(Coauthors.id == Author.id)) \
                       .where(Coauthors.saved == 1) \
                       .order_by(Author.cited_by_count.desc()) \
                       .limit(1200)
    query = """
    SELECT
        author.id,
        coauthors.co_list,
        json_extract(author.affiliation_current, '$.affiliation-country') AS country_origin
    FROM
        author
    INNER JOIN coauthors ON author.id = coauthors.id
    WHERE
        author.cat LIKE '%PHYS%'
        AND country_origin IN ('Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Cyprus', 'Czech Republic', 'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland', 'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden', 'United Kingdom')
        
    ORDER BY
        author.cited_by_count DESC
    OFFSET 100
    LIMIT 100
    """
    authors = db.execute_sql(query)
    
    with ThreadPoolExecutor(max_workers=8) as executor:
        executor.map(build_network, authors, timeout=260)
    
    server.stop()
    exit()