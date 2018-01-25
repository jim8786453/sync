import copy

import sync

from sync import settings
from sync import Text
from sync.exceptions import DatabaseNotFoundError
from sync.storage.base import Storage


mock_storage_objects = {}


class MockStorage(Storage):
    """Store data in-memory."""

    def __init__(self, network_id):
        self.id = network_id

        self.network = None
        self.nodes = {}
        self.messages = {}
        self.errors = {}
        self.changes = {}
        self.records = {}
        self.remotes = {}

    def _save(self, obj, dict_):
        if obj.id is None:
            obj.id = sync.generate_id()

        dict_[obj.id] = copy.deepcopy(obj)

    def connect(self, create_db=False):
        if self.id not in mock_storage_objects and not create_db:
            raise DatabaseNotFoundError()

        if self.id in mock_storage_objects:
            obj = mock_storage_objects[self.id]
            self.network = obj.network
            self.nodes = obj.nodes
            self.messages = obj.messages
            self.errors = obj.errors
            self.changes = obj.changes
            self.records = obj.records
            self.remotes = obj.remotes

        mock_storage_objects[self.id] = self

    def disconnect(self):
        pass

    def drop(self):
        pass

    def start_transaction(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def save_network(self, network):
        if network.id is None:
            network.id = self.id
        self.network = copy.deepcopy(network)

    def save_node(self, node):
        self._save(node, self.nodes)

    def save_message(self, message):
        self._save(message, self.messages)

    def save_change(self, change):
        self._save(change, self.changes)

    def save_record(self, record):
        self._save(record, self.records)

    def save_remote(self, remote):
        self._save(remote, self.remotes)

    def get_network(self):
        return self.network

    def get_node(self, node_id):
        return self.nodes.get(node_id, None)

    def get_record(self, record_id):
        return self.records.get(record_id, None)

    def get_remote(self, node_id, remote_id=None, record_id=None):
        if remote_id is None and record_id is None:
            raise sync.exceptions.InvalidOperationError(
                Text.RemoteOrRecordRequired)

        for r in self.remotes.values():
            if node_id == r.node_id and remote_id is not None and \
               remote_id == r.remote_id:
                return r
            if node_id == r.node_id and record_id is not None and \
               record_id == r.record_id:
                return r
        return None

    def get_remotes(self, record_ids):
        results = []
        for r in self.remotes.values():
            if r.record_id in record_ids:
                results.append(r)
        return results

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.State.Pending, with_for_update=False):
        if message_id is not None:
            return self.messages.get(message_id, None)

        if destination_id is not None:
            for message in self.messages.values():
                if message.state == state and \
                   message.destination_id == destination_id:
                    return message

        return None

    def get_message_count(self, destination_id=None, state=sync.State.Pending):
        result = 0
        for message in self.messages.values():
            if message.state == state and \
               message.destination_id == destination_id:
                    result = result + 1

        return result

    def get_nodes(self):
        return list(self.nodes.values())

    def get_records(self):
        records = []
        for r in self.records.values():
            if not r.deleted:
                records.append(r)

        results = {}
        for record in records:
            results[record.id] = record

        remotes = self.get_remotes(results.keys())
        for remote in remotes:
            results[remote.record_id]._remotes.append(remote)

        return [results.values()]

    def get_changes(self, message_id):
        results = []
        for c in self.changes.values():
            if message_id == c.message_id:
                results.append(c)
        return results

    def update_messages(self, node_id, record_id, remote_id):
        for message in self.messages.values():
            if message.state == sync.State.Pending \
               and message.destination_id == node_id \
               and message.record_id == record_id:
                message.remote_id = remote_id
