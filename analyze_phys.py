import pandas as pd
import networkx as nx
import os
import csv
import matplotlib.pyplot as plt
from networkx.algorithms.community import k_clique_communities
import pygraphviz
from networkx.drawing.nx_agraph import graphviz_layout
from itertools import groupby
import numpy as np
from nxviz import CircosPlot
from nxviz.plots import ArcPlot, MatrixPlot
import yaml

# ## Connectivity between countries
# We need to compute connectivity of coauthors from these countries

# List of official countries names
with open('./countries.yaml', 'r') as f:
    countries = list((yaml.load(f))['countries'].values())
    countries.extend(['Moldova','South Korea'])

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

# ## Build network of a specifiecd ego
# Network will be multilayered by years of collaboration between actors (authors)
def load_network(id, nodes, edges):
    graph = nx.MultiGraph()
    for node in nodes:
        graph.add_node(node['Id'], label=node['Name'], university=node['University'], country=node['Country'], citations=int(node['Citations']))
    for edge in edges:
        graph.add_edge(edge['Source'], edge['Target'], weight=int(edge['Weight']), year=edge['Year'])

    try:
        graph.remove_node(id)
    except:
        #nothing
        print('{} is not present in network'.format(id))

    return graph

def draw_graph(graph):
    # pos = graphviz_layout(graph, prog='twopi', args='')
    # plt.figure(figsize=(8, 8))
    # nx.draw(graph, pos, node_size=20, alpha=0.5, node_color="blue", with_labels=False)
    # plt.axis('equal')
    # plt.show()

    # options = {
    #     'node_color': 'black',
    #     'node_size': 50,
    #     'line_color': 'grey',
    #     'linewidths': 0,
    #     'width': 0.1,
    # }
    # nx.draw(graph, **options)
    # plt.show()

    # Assume we have a professional network of physicians belonging to hospitals.
    # c = CircosPlot(graph, node_color='university', node_grouping='university')
    c = ArcPlot(graph, node_color="country", node_grouping="university", group_order="alphabetically")
    c.draw()
    plt.show()  # only needed in scripts


def analyze_graph(graph):
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.cluster.triangles.html
    # Triangles per nodes, we should analyse the average per graph 
    triangles = np.average(list(nx.triangles(graph).values()))
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.cluster.transitivity.html
    transitivity = nx.transitivity(graph)
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.cluster.clustering.html
    # clustering = nx.clustering(graph, weight='weight').values()
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.cluster.average_clustering.html
    average_clustering = nx.average_clustering(graph, weight='weight', count_zeros=False)
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.bipartite.centrality.closeness_centrality.html
    closeness = nx.closeness_centrality(graph).values()
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.bipartite.centrality.betweenness_centrality.html
    betweenness = nx.betweenness_centrality(graph).values()
    # https://networkx.github.io/documentation/latest/reference/algorithms/generated/networkx.algorithms.assortativity.degree_assortativity_coefficient.html
    homophily = nx.degree_assortativity_coefficient(graph, weight='weight')
    # https://networkx.github.io/documentation/networkx-1.9.1/reference/generated/networkx.algorithms.assortativity.attribute_assortativity_coefficient.html
    # Homophily by citations
    homophily_citations = nx.attribute_assortativity_coefficient(graph, 'citations')
    # Homophily by university
    homophily_university = nx.attribute_assortativity_coefficient(graph, 'university')

    return {
        'triangles': np.round(triangles, 2),
        'transitivity': transitivity,
        # 'clustering': clustering,
        'average_clustering': average_clustering,
        'closeness': list(closeness),
        'betweenness': list(betweenness),
        'homophily': homophily,
        'homophily_citations': homophily_citations,
        'homophily_university': homophily_university
    }

def build_analysis():
    n_list = pd.read_csv('phys_networks.csv', usecols=[0])

    phys_data = pd.DataFrame(columns=[
        'id', 'triangles', 'transitivity', 'closeness', 'betweenness',
        'average_clustering', 'homophily', 'homophily_citations', 'homophily_university'])

    for i,author in n_list.iterrows():
        try: 
            nodes,edges = get_network(author['id'])
            if nodes and edges:
                data = analyze_graph(load_network(author['id'], nodes,edges))
                data['id'] = author['id']
                phys_data = phys_data.append(data, ignore_index=True)
                print('.', end="")
        except BaseException as e:
            print('')
            print('Network id: {}'.format(author['id']))
            print(e)

    phys_data.to_pickle('./obj/phys_data.pkl')

# ## Connectivity between countries
# We need to compute connectivity of coauthors from these countries
def group_edges_by_country(source_country,nodes,edges):
    inner_nodes = [n for n in nodes if n['Country'] == source_country]
    external_nodes = [n for n in nodes if n['Country'] != source_country]

    inner_nodes_ids = [n['Id'] for n in inner_nodes]
    external_nodes_ids = [n['Id'] for n in external_nodes]

    grouped = {}

    for edge in edges:
        # Check if current connection it's between the current country and an eternal country
        if edge['Source'] in inner_nodes_ids and edge['Target'] in external_nodes_ids:
            country = (next(n for n in external_nodes if n['Id'] == edge['Target']))['Country']
            if country in grouped:
                grouped[country].append(edge)
            else:
                grouped[country] = [edge]
        
        if edge['Source'] in external_nodes_ids and edge['Target'] in inner_nodes_ids:
            country = (next(n for n in external_nodes if n['Id'] == edge['Source']))['Country']
            if country in grouped:
                grouped[country].append(edge)
            else:
                grouped[country] = [edge]

    return grouped


def load_country_network(name):
    n_list = pd.read_csv('./phys_networks.csv', usecols=[0])
    big_nodes = []
    big_edges = []
    external_edges = {}

    for i,author in n_list.iterrows():
        try: 
            # Parse network files with nodes and edges
            nodes,edges = get_network(author['id'])
            # Check if network it's valid (has nodes and edges)
            if len(nodes) > 0 and len(edges) > 0:
                # Group connections by country
                for country,conns in (group_edges_by_country(name, nodes, edges)).items():
                    if country in external_edges:
                        external_edges[country].extend(conns)
                    else:
                        external_edges[country] = conns

                inner_nodes = [
                    n for n in nodes
                    if n['Country'] == name
                ]

                inner_nodes_ids = [n['Id'] for n in inner_nodes]

                inner_edges = [
                    e for e in edges
                    if e['Source'] in inner_nodes_ids and e['Target'] in inner_nodes_ids
                ]
                
                big_nodes.extend(inner_nodes)
                big_edges.extend(inner_edges)

        except BaseException as e:
            print('Error in Network id: {}'.format(author['id']))
            raise e

    return big_nodes,big_edges,external_edges


def save_country_network(name):
    # Write country network to file
    g_nodes,g_edges,g_extern = load_country_network(name)

    with open('./country_networks/{}_nodes.csv'.format(name), 'w', newline='') as csvfile:
        fieldnames = ['Id','Name','Country','City','University','Citations']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for node in g_nodes:
            writer.writerow(node)
        
    with open('./country_networks/{}_edges.csv'.format(name), 'w', newline='') as csvfile:
        fieldnames = ['Source','Target','Year','Weight','Keywords']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for edge in g_edges:
            writer.writerow(edge)

    with open('./country_networks/{}_external.csv'.format(name), 'w', newline='') as csvfile:
        fieldnames = ['Country','Source','Target','Year','Weight','Keywords']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for country,edges in g_extern.items():
            for edge in edges:
                row = edge
                row['Country'] = country
                writer.writerow(edge)


def export_country_network_data(name):
    g_nodes,g_edges,g_extern = [],[],[]
    g_nodes = pd.read_csv('./country_networks/{}_nodes.csv'.format(name))
    g_edges = pd.read_csv('./country_networks/{}_edges.csv'.format(name))
    g_edges['Year'] = pd.to_numeric(g_edges['Year'])
    g_extern = pd.read_csv('./country_networks/{}_external.csv'.format(name))
    g_extern['Year'] = pd.to_numeric(g_extern['Year'])
    # Keep only edges after 2006 
    g_edges = g_edges[(g_edges['Year'] > 2006) & (g_edges['Year'] < 2018)]
    g_extern = g_extern[(g_extern['Year'] > 2006) & (g_extern['Year'] < 2018)]

    # Country network node data variables
    # Id, Name, Year, number of nodes, number of edges, connectivity, 
    # ['Germany-2007', 'Germany', 2007, 100, 1000, 0.05]
    country_per_year = []
    
    for year,group in g_edges.groupby('Year'):
        compute_nodes = np.unique(np.array([[e['Source'],e['Target']] for i,e in group.iterrows()]).flatten())
        n_number = len(compute_nodes)
        compute_edge_connectivity = (len(group) / ((n_number*n_number-1)/2))
        country_per_year.append(["{}-{}".format(name,year), name, year, n_number, len(group), compute_edge_connectivity])
    
    # COuntry network edges data variables
    # Current country, extern country, year, number of edges, percent of nodes connected
    # ['Germany', 'Sweeden', 2007, 100, 25]
    external_per_year = []

    for country,edges in g_extern.groupby('Country'):
        for year,group in edges.groupby('Year'):
            total_nodes = next((n[3] for n in country_per_year if n[2] == year), 0)
            compute_nodes = len(np.unique(np.array([[e['Source'],e['Target']] for i,e in group.iterrows()]).flatten()))

            if total_nodes > 0:
                compute_percent = (compute_nodes * 100)/total_nodes
            else:
                compute_percent = 0
            external_per_year.append(["{}-{}".format(name,year), "{}-{}".format(country,year), year, len(group), compute_percent])

    with open('./country_data/nodes.csv', 'a', newline='') as csvfile:
        fieldnames = ['Country','Year','Nodes','Edges','Connectivity']
        writer = csv.writer(csvfile, dialect='excel')
        writer.writerows(country_per_year)

    with open('./country_data/edges.csv', 'a', newline='') as csvfile:
        fieldnames = ['Country','E. Country','Year','Edges','Percent of Nodes C.']
        writer = csv.writer(csvfile, dialect='excel')
        writer.writerows(external_per_year)

if __name__=='__main__':
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

    # for country in countries:
    #     if not os.path.exists('./country_networks/{}_nodes.csv'.format(country)):
    #         save_country_network(country)
    #     export_country_network_data(country)

    export_country_network_data('United Kingdom')


# SELECT *, count(id) as weight FROM 
# (SELECT * FROM collaborations_expanded WHERE author_id = '2'
# INTERSECT
# SELECT * FROM collaborations_expanded WHERE author_id = '1')
# GROUP BY year