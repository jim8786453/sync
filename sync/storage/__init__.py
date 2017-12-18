import sys

import sync

from sync import settings
from sync.storage.base import Storage
from sync.storage.mongo import MongoStorage
from sync.storage.mock import MockStorage
from sync.storage.postgres import PostgresStorage


def init_storage(network_id, create_db=False):
    """Instantiate the storage object and pass init the sync network.

    """
    current_module = sys.modules[__name__]
    storage_class = getattr(current_module, settings.STORAGE_CLASS)
    storage = storage_class(network_id)
    storage.connect(create_db=create_db)
    sync.init(storage)


__all__ = ["init_storage", "MockStorage", "MongoStorage",
           "PostgresStorage", "Storage"]
