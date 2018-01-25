import six

from pymongo import MongoClient

import sync

from sync import settings
from sync import Text
from sync.exceptions import DatabaseNotFoundError
from sync.storage.base import Storage


# Used to store a mock Mongodb client.
test_mongo_client = None


class MongoStorage(Storage):
    """Store data in a Mongo database."""

    def __init__(self, network_id):
        self.id = network_id
        self.session = None

    def _get_one(self, table, filter_, class_, sort=None):
        record = self.session[table].find_one(filter_, sort=sort)

        if record is None:
            return None

        obj = class_()
        for key in record.keys():
            if not key == '_id':
                setattr(obj, key, record[key])
            if table == 'networks':
                obj.schema = self._decode_dollar_prefix(obj.schema)

        return obj

    def _get_many(self, table, filter_, class_):
        rows = self.session[table].find(filter_)

        results = []
        for row in rows:
            obj = class_()
            for key in row.keys():
                if not key == '_id':
                    setattr(obj, key, row[key])
            results.append(obj)

        return results

    def _encode_dollar_prefix(self, dict_value):
        """To store JSON schema in Mongodb we need to escape any fields that
        are prefixed with dollar characters ($) as these are reserved
        in Mongodb.

        """
        if dict_value is None:
            return dict_value
        for key, value in six.iteritems(dict_value):
            if key.startswith('$'):
                encoded_key = '__dollar__' + key
                dict_value[encoded_key] = dict_value[key]
                del(dict_value[key])
            if isinstance(value, dict):
                dict_value[key] = self._encode_dollar_prefix(value)
        return dict_value

    def _decode_dollar_prefix(self, dict_value):
        """To store JSON schema in Mongodb we needed to escape any fields that
        are prefixed with dollar characters ($) as these are reserved
        in Mongodb. This function restores the original key.

        """
        if dict_value is None:
            return dict_value
        for key, value in six.iteritems(dict_value):
            if key.startswith('__dollar__'):
                decoded_key = key.replace('__dollar__', '')
                dict_value[decoded_key] = dict_value[key]
                del(dict_value[key])
            if isinstance(value, dict):
                dict_value[key] = self._decode_dollar_prefix(value)
        return dict_value

    def _save(self, table, obj, override_id=False):
        values = obj.as_dict(False)

        if table == 'networks':
            values['schema'] = self._encode_dollar_prefix(values['schema'])

        if not hasattr(obj, 'id') or obj.id is None:
            obj.id = override_id or sync.generate_id()
            values['id'] = obj.id
            self.session[table].insert_one(values)
        else:
            filter_ = {
                'id': obj.id
            }
            values = {
                '$set': values
            }
            self.session[table].update_one(filter_, values)

    def _database_exists(self):
        names = self.client.database_names()
        return self.id in names

    def connect(self, create_db=False):
        self.base_url = settings.MONGO_CONNECTION
        self.client = test_mongo_client or MongoClient(self.base_url)

        if not self._database_exists() and not create_db:
            raise DatabaseNotFoundError()

        self.session = self.client[self.id]

    def disconnect(self):
        self.client.close()

    def drop(self):
        self.client.drop_database(self.id)

    def start_transaction(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def save_network(self, network):
        self._save('networks', network, self.id)

    def save_node(self, node):
        self._save('nodes', node)

    def save_message(self, message):
        self._save('messages', message)

    def save_change(self, change):
        self._save('changes', change)

    def save_record(self, record):
        self._save('records', record)

    def save_remote(self, remote):
        self._save('remotes', remote)

    def get_network(self):
        return self._get_one('networks', {}, sync.Network)

    def get_node(self, node_id):
        filter_ = {
            'id': node_id
        }
        return self._get_one('nodes', filter_, sync.Node)

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.State.Pending,
                    with_for_update=False):
        filter_ = {}

        if message_id is not None:
            filter_['id'] = message_id
        elif destination_id is not None:
            filter_['state'] = state
            filter_['destination_id'] = destination_id

        sort = [('timestamp', 1)]

        return self._get_one('messages', filter_, sync.Message, sort)

    def get_message_count(self, destination_id=None, state=sync.State.Pending):
        filter_ = {}
        filter_['state'] = state
        filter_['destination_id'] = destination_id
        count = self.session['messages'].find(filter_).count()
        return count

    def get_record(self, record_id):
        filter_ = {
            'id': record_id
        }

        record = self._get_one('records', filter_, sync.Record)

        if record is None:
            return None

        record._remotes = self.get_remotes([record_id])

        return record

    def get_remote(self, node_id, remote_id=None, record_id=None):
        if remote_id is None and record_id is None:
            raise sync.exceptions.InvalidOperationError(
                Text.RemoteOrRecordRequired)

        filter_ = {
            'node_id': node_id
        }
        if remote_id is not None:
            filter_['remote_id'] = remote_id
        elif record_id is not None:
            filter_['record_id'] = record_id

        return self._get_one('remotes', filter_, sync.Remote)

    def get_remotes(self, record_ids):
        filter_ = {
            'record_id':  {
                '$in': record_ids
            }
        }
        return self._get_many('remotes', filter_, sync.Remote)

    def get_nodes(self):
        return self._get_many('nodes', {}, sync.Node)

    def get_records(self):
        """Fetch all records using a generator.
        :returns: Batches of records.
        :rtype: generator
        """
        filter_ = {
            'deleted': False
        }
        skip = 0
        limit = 1

        while True:
            # Use a dictionary so that the associated remote objects
            # can easily be added into the appropriate
            # 'record.remotes' object cache.
            results = {}

            # Fetch a batch of records.
            rows = self.session['records'].find(filter_, skip=skip,
                                                limit=limit)
            skip = skip + limit
            chunk = []
            for row in rows:
                obj = sync.Record()
                for key in row.keys():
                    if not key == '_id':
                        setattr(obj, key, row[key])
                chunk.append(obj)

            if len(chunk) == 0:
                break

            for obj in chunk:
                results[obj.id] = obj

            # Efficiently fetch the associated remote objects for
            # this batch of records.
            remotes = self.get_remotes(list(results.keys()))

            # Add the remote objects to the associated record.remotes
            # cache.
            for remote in remotes:
                results[remote.record_id]._remotes.append(remote)

            yield results.values()

    def get_changes(self, message_id):
        filter_ = {
            'message_id': message_id
        }
        return self._get_many('changes', filter_, sync.Change)

    def update_messages(self, node_id, record_id, remote_id):
        filter_ = {
            'destination_id': node_id,
            'record_id': record_id
        }
        values = {
            '$set': {'remote_id': remote_id}
        }
        self.session['messages'].update_many(filter_, values)
