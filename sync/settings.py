import os


"""STORAGE_CLASS: the backend database to use.

- MockStorage
- PostgresStorage

"""
STORAGE_CLASS = os.environ.get('STORAGE_CLASS', None)
if STORAGE_CLASS is None:
    STORAGE_CLASS = 'PostgresStorage'


"""POSTGRES_CONNECTION: if using the PostgresStorage backend a base
connection string is required. As there is one sync system per
database the database name will be appended to this value during
initialisation.

E.g. postgresql://user:pass@localhost:5432/

"""
POSTGRES_CONNECTION = os.environ.get('POSTGRES_CONNECTION', None)
if POSTGRES_CONNECTION is None:
    POSTGRES_CONNECTION = 'postgresql://sync:sync@localhost:5432/'
