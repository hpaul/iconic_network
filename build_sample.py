from models import *
# Print all queries to stderr.
import logging
logger = logging.getLogger('peewee')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

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

for country in countries:
    authors = db.execute_sql("UPDATE author SET is_sample = 1 WHERE id in (SELECT id FROM author WHERE country LIKE '%" + country + "%' LIMIT 1000)")
    
    print(len(authors.fetchall()))