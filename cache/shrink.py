from playhouse.sqlite_ext import *
from playhouse.migrate import *
from peewee import *
import os

# The initialisation
db = SqliteDatabase('./iconic.db', pragmas={
    'journal_mode': 'wal',
    'cache_size': '-2000',
    'fullfsync': 'on',
    'journal_size_limit': '-1',
    'threads': '8',
    'foreign_keys': 1,  # Enforce foreign-key constraints
})

db.connect()

db.close()