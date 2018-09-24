from playhouse.sqlite_ext import *
from playhouse.migrate import *
from peewee import *

# The initialisation
db = SqliteDatabase('./iconic.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': '-2000',
    'fullfsync': 'on',
    'journal_size_limit': '-1',
    'threads': '8',
    'foreign_keys': 1,  # Enforce foreign-key constraints
})

# The initialisation
#network = MySQLDatabase('coauthorship', user='root', host='127.0.0.1', password='pass')

class BaseModel(Model):
    class Meta:
        database = db


# class BaseNetwork(Model):
#     class Meta:
#         database = network

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
    last_page = BigIntegerField(null=True,default=0)
    is_sample = BooleanField(default=False)


class Collaboration(BaseModel):
    abs_id = BigIntegerField(unique=True, index=True, primary_key=True)
    authors = JSONField(null=True)
    published = DateField(null=True)
    cited_by = IntegerField(null=True)
    keywords = JSONField(null=True)
    coll_count = IntegerField(null=True)
    message = TextField(null=True)
    saved = BooleanField(default=False)

class Coauthors(BaseModel):
    id = BigIntegerField(unique=True, index=True, primary_key=True)
    co_list = JSONField(null=True)
    last_page = IntegerField(null=True)
    saved = BooleanField(default=False)

# class AuthorDetails(BaseNetwork):
#     id = BigAutoField(unique=True, index=True, primary_key=True)
#     full_name = TextField(null=True)
#     preferred_name = TextField(null=True)
#     affiliation_id = BigIntegerField(unique=True, index=True, null=True)
#     url = TextField(null=True)


# class Network(BaseNetwork):
#     id = BigAutoField(unique=True, index=True, primary_key=True)
#     from_author = BigIntegerField(index=True)
#     to_author = BigIntegerField(index=True)
#     article = BigIntegerField(index=True)
#     keywords = JSONField(null=True)
#     year = IntegerField(null=True)
#     citations = IntegerField(null=True)


# class Affiliation(BaseNetwork):
#     url = TextField(null=True)
