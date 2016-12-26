import os


"""STORAGE_CLASS: the backend database to use.

- MockStorage
- PostgresStorage
- MongoStorage

"""
STORAGE_CLASS = os.environ.get('STORAGE_CLASS', None)
if STORAGE_CLASS is None:
    STORAGE_CLASS = 'MockStorage'


"""POSTGRES_CONNECTION: if using the PostgresStorage backend a base
connection string is required. As there is one sync system per
database the database name will be appended to this value during
initialisation.

E.g. postgresql://username:pass@localhost:5432/

"""
POSTGRES_CONNECTION = os.environ.get('POSTGRES_CONNECTION', None)
if POSTGRES_CONNECTION is None:
    POSTGRES_CONNECTION = 'postgresql://username:pass@localhost:5432/'

"""MONGO_CONNECTION: if using the MongoStorage backend a base
connection string is required.

E.g. mongodb://localhost:27017/

"""
MONGO_CONNECTION = os.environ.get('MONGO_CONNECTION', None)
if MONGO_CONNECTION is None:
    MONGO_CONNECTION = 'mongodb://localhost:27017/'
