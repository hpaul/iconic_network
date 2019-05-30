import pandas as pd
import os
import csv
import matplotlib.pyplot as plt
from itertools import groupby
import numpy as np
import yaml
import psycopg2

# List of official countries names
with open('./countries.yaml', 'r') as f:
    countries = list((yaml.load(f))['countries'].values())
    countries.extend(['Moldova','South Korea'])


def insert_author(author):
    sql = "INSERT INTO authors(id,name,country,city,university,citations) VALUES(%s,%s,%s,%s,%s,%s)"
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(database='iconic', user='hpaul')
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql,author)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_edge(edge):
    sql = "INSERT INTO edges(source,target,year,weight,keywords) VALUES(%s,%s,%s,%s,%s)"
    conn = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(database='iconic', user='hpaul')
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql,edge)
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_country_name(data):
    computed = ''
    for name in data.split(' '):
        computed = ("{} {}".format(computed, name)).strip()
        if computed in countries:
            return computed
        else:
            continue

    return data


# # Get an network of a specified ego
def get_network(id):
    nodes = []
    edges = []
    node_file, edge_file = ('networks/' + str(id) + '_nodes.csv', 'networks/' + str(id) + '_edges.csv')

    if os.path.exists(node_file) and os.path.exists(edge_file):
        with open(node_file, 'r') as reader:
            csvreader = csv.DictReader(reader, dialect='excel')
            for row in csvreader:
                # TODO: Normalize attributes (country especially)
                row['Country'] = get_country_name(row['Country'])
                nodes.append(row)

        with open(edge_file, 'r') as reader:
            csvreader = csv.DictReader(reader, dialect='excel')
            for row in csvreader:
                edges.append(row) 

    return nodes,edges

def insert_network():
    n_list = pd.read_csv('./phys_networks.csv', usecols=[0])

    for i,author in n_list.iterrows():
        try: 
            # Parse network files with nodes and edges
            nodes,edges = get_network(author['id'])
            # Check if network it's valid (has nodes and edges)
            if len(nodes) > 0 and len(edges) > 0:
                for node in nodes:
                    data = (node['Id'], node['Name'], node['Country'], node['City'], node['University'], node['Citations'],)
                    insert_author(data)
                    print('.', end='')

                print('')
                print('{}: Authors inserted, time for edges!'.format(author['id']))

                for edge in edges:
                    data = (edge['Source'],edge['Target'],edge['Year'],edge['Weight'],edge['Keywords'],)
                    insert_edge(data)
                    print('.', end='')

                print('')
                print('{}: Edges inserted, done!'.format(author['id']))
        
        except BaseException as e:
            print('Error in Network id: {}'.format(author['id']))
            raise e


def build_data():
    query = """
    SELECT
        year,
        sum(edges.weight) AS total
    FROM
        edges
    WHERE
        edges.source IN (
            SELECT
                id
            FROM
                authors
            WHERE
                country = '%s')
            OR edges.target IN (
                SELECT
                    id
                FROM
                    authors
                WHERE
                    country = '%s')
            GROUP BY
                edges.year
            ORDER BY
                edges.year DESC
    """

    conn = psycopg2.connect(database='iconic', user='hpaul')
    countries = pd.read_sql_query("SELECT country, count(id) as total_authors FROM authors GROUP BY country",conn)
    countries.to_csv('final_data/countries_authors.csv')
    print(countries.head())

if __name__=='__main__':
    build_data()
