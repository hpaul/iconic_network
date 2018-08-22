from models import *
from peewee import *

db.create_tables([Author, AuthorDetails, Collaboration, Network, Affiliation], )

# Migrations
docs_fetched = BooleanField(default=False)
last_page = BigIntegerField(null=True,default=0)
saved = BooleanField(default=False)

migrator = SqliteMigrator(db)
migrate(
    #migrator.add_column('author', 'docs_fetched', docs_fetched),
    #migrator.add_column('author', 'last_page', last_page),
    #migrator.add_column('collaboration', 'saved', saved)
)