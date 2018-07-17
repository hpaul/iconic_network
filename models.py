from playhouse.sqlite_ext import *
from playhouse.migrate import *
from peewee import *

# The initialisation
db = SqliteDatabase('./iconic.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': 10000,  # 10000 pages, or ~40MB
    'foreign_keys': 1,  # Enforce foreign-key constraints
})


class BaseModel(Model):
    class Meta:
        database = db


class Author(BaseModel):
    id = BigIntegerField(unique=True, index=True, primary_key=True)
    full_name = JSONField(null=True)
    subject_areas = JSONField(null=True)
    document_count = BigIntegerField(null=True)
    cited_by_count = BigIntegerField(null=True)
    citations_count = BigIntegerField(null=True)
    h_index = BigIntegerField(null=True)
    coauthors_count = BigIntegerField(null=True)
    # Data about affiliation
    affiliation_current = JSONField(null=True)
    cat = JSONField(null=True)
    country = JSONField(null=True)
    docs_fetched = BooleanField(default=False)


class Collaboration(BaseModel):
    author = BigIntegerField(index=True)
    co_author = BigIntegerField(index=True)
    year = IntegerField(null=True)
    cited_by = IntegerField(null=True)
    keywords = JSONField(null=True)
    coll_count = IntegerField(null=True)


# Initialize database
db.connect()
db.create_tables([Author, Collaboration], )
db.close()

# Migrations
# docs_fetched = BooleanField(default=False)
#
# migrator = SqliteMigrator(db)
# migrate(
#     migrator.add_column('author', 'docs_fetched', docs_fetched)
# )
