from models import *
import dask
import itertools
import pyorient

client = pyorient.OrientDB("localhost", 2424)
client.db_open( "coauthors", "admin", "admin" )

class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def save_author(id, data):
    result = client.query("select from Author WHERE Author.id = %s" % id, 1, '*:0')

    if len(result) == 0:
        auth = { '@Authors': { 'id': id, 'name': data.get('authname') } }
        rec_position = client.record_create(rec)


def save_network(from_author, data):

    q = "CREATE EDGE Wrote FROM (SELECT FROM Authors WHERE id = %s) TO \
          (SELECT FROM Article WHERE id = %s)" % (from_author, data.abs_id)
    client.query(q)


def save_collaboration(row):
    keywords = str(row.keywords or '')
    keywords = keywords.replace('"', '')

    rec = { '@Article': { 'id': row.id, 'published': row.published, 'citations': row.cited_by, 'keywords': keywords } }
    rec_position = client.record_create(rec)

    lazy_network = []
    
    print(Colour.BOLD + str(row.abs_id) + ' - Saving authors' + Colour.END)
    # Store each author details
    for author in row.authors:
        id = author.get('authid')

        if id is not None:
            s_author = dask.delayed(save_author)(id, author)
            lazy_network.append(s_author)

    print('')
    print(Colour.OKGREEN + str(row.abs_id) + ' - Saved' + Colour.END)

    print(Colour.BOLD + str(row.abs_id) + ' - Saving network of authors' + Colour.END)
    auth_ids = [ath.get('authid') for ath in row.authors if ath.get('authid') is not None]
    # Generate network between article authors
    #comb = itertools.combinations(auth_ids, 2)

    for from_author in auth_ids:
        s_network = dask.delayed(save_network)(from_author, row)
        lazy_network.append(s_network)

    print('')
    print(Colour.OKGREEN + str(row.abs_id) + ' - Saved' + Colour.END)

    fetched = Collaboration.update(saved=True).where(Collaboration.abs_id == row.abs_id)
    fetched.execute()

    # Save network
    dask.compute(*lazy_network)


def build_network():
    lazy_collaborations = []
    collaborations = Collaboration.select().order_by(Collaboration.cited_by.desc()).where(Collaboration.saved == 0).limit(1).iterator()

    for collaboration in collaborations:
        task = dask.delayed(save_collaboration)(collaboration)
        lazy_collaborations.append(task)

    dask.compute(*lazy_collaborations)


net = build_network()


# def connect_author(id):
#     def create(tx, aid):
#         return tx.run("MATCH (seed:Author {id:$id})-[:AUTHORED]->(ar:Article)<-[:AUTHORED]-(co)"
#                       "MERGE (seed)-[r:COAUTHOR]->(co)"
#                       "RETURN id(r)", id=aid)
    
#     ids = []
#     with driver.session() as session:
#         ids = session.write_transaction(create, id)
    
#     print(Colour.OKGREEN + str(id) + ' - End buliding' + Colour.END)
#     return ids

# def build_coauthors():

#     lazy_queue = []
#     # Get list o author
#     # Iterate over each one and build own coauthorship
#     def get_authors(tx):
#         return tx.run("MATCH (a:Author) RETURN a.id")

#     results = []

#     with driver.session() as session:
#         authors = session.read_transaction(get_authors)

#         for author in authors:
#             print(Colour.OKGREEN + str(author["a.id"]) + ' - Building..' + Colour.END)
#             task = dask.delayed(connect_author)(author["a.id"])
#             lazy_queue.append(task)

#         results = dask.compute(*lazy_queue)

#     return results

# co = build_coauthors()
# print(co)