from models import *
import dask
import itertools
import csv
import json
import logging
import os.path
import time
from dateutil import parser
import pprint as pp
# logger = logging.getLogger('peewee')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())


class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def save_collaboration(row, author_nodes, author_edges):
    nodes_tasks = []
    edges_tasks = []

    keywords = str(row['keywords'] or '')
    keywords = keywords.replace('"', '')

    print(Colour.BOLD + str(row['abs_id']) + ' - Saving authors' + Colour.END)
    
    auth_ids = [int(ath.get('authid'))
                    for ath in row['authors']
                        if ath.get('authid') is not None and
                        int(ath['authid']) in row['co_list']
                ]

    # Store each author details
    for id in auth_ids:
        try:
            # Try to find details about author in authors table
            author = Author.get(Author.id == id)
            university = author.affiliation_current
            university = university or {}
            node = [
                id,
                university.get('affiliation-country'),
                university.get('affiliation-name')
            ]
            # Insert data row into CSV
            author_nodes.writerow(node)
        except Author.DoesNotExist:
            node = [id, '', '']
            # Insert data row into CSV
            author_nodes.writerow(node)


    print(Colour.BOLD + str(row['abs_id']) + ' - Saving network of authors' + Colour.END)
    
    # Generate network between article authors
    comb = itertools.combinations(auth_ids, 2)

    for source, target in comb:
        edge = [
            source,
            target,
            row['published'].year, 
            row['cited_by']
        ]
        # Insert data into CSV
        author_edges.writerow(edge)

    print('')
    print(Colour.OKGREEN + str(row['abs_id']) + ' - Saved' + Colour.END)


def with_file(filename, callback):
    with open(filename, 'w+') as f:
        return callback(f)


def build_networks():
    authors = Coauthors.select(Coauthors.id, Coauthors.co_list, Author.cited_by_count).join(Author, on=(Coauthors.id == Author.id)).where(Coauthors.saved == 1).order_by(Author.cited_by_count.desc())
    
    for author in authors:
        print(Colour.BOLD + "Build network for: " + str(author) + Colour.END)
        lazy_network = []

        nodes = "data/%s_nodes.csv" % author.id
        edges = "data/%s_edges.csv" % author.id
        
        if os.path.exists(nodes) is False and os.path.exists(edges) is False:
            with open(nodes, "wt") as nodes, open(edges, "wt") as edges:
                # CSV with nodes and columns
                nodes.truncate(0)
                author_nodes = csv.writer(nodes, 'excel')
                author_nodes.writerow(('Id', 'country', 'university'))
                
                # CSV with edges columns
                edges.truncate(0)
                author_edges = csv.writer(edges, 'excel')
                author_edges.writerow(['Source', 'Target', 'year', 'citations'])

                def save_articles(coauth):
                    print(Colour.BOLD + " -----articles for coauthor: " + str(coauth) + Colour.END)
                    articles = Collaboration.select().where(Collaboration.authors_id.contains(coauth))
  
                    print(Colour.OKGREEN + "-GOT THEM: " + str(coauth) + Colour.END)
                    for collaboration in articles:
                        co_list = author.co_list
                        co_list.append(author.id)

                        coll = {
                            'abs_id': collaboration.abs_id,
                            'authors': collaboration.authors,
                            'published': collaboration.published,
                            'cited_by': collaboration.cited_by,
                            'keywords': collaboration.keywords,
                            'co_list': set(co_list)
                        }
                        save_collaboration(coll, author_nodes, author_edges)
                    

                for coauthor in author.co_list:
                    task = dask.delayed(save_articles)(coauthor)
                    lazy_network.append(task)

                dask.compute(*lazy_network)

# Get list of most cited authors and their co-authors
# For each co-author get his documents
# Go in each document and build the network
# Select author co-authors and exclude others
# Save nodes in *id*_nodes.csv
# Save edges in *id*_edges.csv

build_networks()