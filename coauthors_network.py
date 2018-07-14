""" Elsevier data downloader """

from requests import Request
from client import ElsClient, ElsSearch
import csv
import json
import re

class Colour:
	OKGREEN = '\033[92m'
	BOLD = '\033[1m'
	UNDERLINE = '\033[4m'
	END = '\033[0m'

# Limit number of articles
# limit = 5187
limit = 150

# We only need publications between 2007 and 2018
years = 'PUBYEAR > 2006 AND PUBYEAR < 2019'

## Load configuration
con_file = open("config.json")
config = json.load(con_file)
con_file.close()

## Initialize elsclient client
client = ElsClient(config['apikey'])

# Open the list with our authors
authors = open('authors_list.csv', 'r', encoding='utf8')
author_list = authors.reader(csv_file, dialect=csv.excel)

for author in author_list:
    id = author[0].split('/')
    id = id[-1]

    payload = {
        'query': ''.format(author[:, subject)
    }

for country in countries:
	for subject in cats:
		total_saved = 0

		

		print('Getting authors in: "{0} {1}"'.format(country, subject).ljust(10), end="", flush=True)

		p = Request('GET', 'https://api.elsevier.com/content/search/author', params=payload).prepare()

		search = ElsSearch(p.url)
		search.execute(client)
		print(Colour.BOLD + "Data loaded" + Colour.END)

		for author in search.results:
			print(".", end="", flush=True)
			author_csv.writerow([author['prism:url'], country, subject])
		
		total_saved += search.num_res

		while (total_saved < limit):
			for e in search.links:
				if e['@ref'] == 'next':
						next_url = e['@href']
			
			search = None
			search = ElsSearch(next_url)
			search.execute(client)

			for author in search.results:
				print(".", end="", flush=True)
				author_csv.writerow([author['prism:url'], country, subject])

			# Increment saved
			total_saved += search.num_res

		print(Colour.OKGREEN + "DONE" + Colour.END)
