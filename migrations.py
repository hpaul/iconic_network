from models import *
from peewee import *

db.create_tables([Author, Collaboration, Coauthors])

# Migrations
docs_fetched = BooleanField(default=False)
last_page = BigIntegerField(null=True,default=0)
saved = BooleanField(default=False)
is_sample = BooleanField(default=False)
citations = JSONField(null=True)

migrator = SqliteMigrator(db)
migrate(
    #migrator.add_column('author', 'docs_fetched', docs_fetched),
    #migrator.add_column('author', 'last_page', last_page),
    #migrator.add_column('collaboration', 'saved', saved)
    #migrator.add_column('author', 'is_sample', is_sample)
    migrator.add_column('author', 'citations', citations)
)
