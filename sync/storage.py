import copy

import sqlalchemy as sqla

from pymongo import MongoClient
from sqlalchemy.dialects import postgresql
from sqlalchemy_utils import database_exists, create_database, drop_database

import sync

from sync import settings
from sync.constants import Text
from sync.exceptions import DatabaseNotFoundError


def init_storage(system_id, create_db=False):
    if settings.STORAGE_CLASS == 'PostgresStorage':
        args = {
            'base_connection': settings.POSTGRES_CONNECTION
        }
        init_postgresql(args, system_id, create_db=create_db)
    elif settings.STORAGE_CLASS == 'MockStorage':
        init_mock(system_id, create_db)
    elif settings.STORAGE_CLASS == 'MongoStorage':
        init_mongo(system_id, create_db)


def init_mock(system_id, create_db=False):
    storage = PostgresStorage(system_id)
    sync.init(storage, create_db)


def init_postgresql(settings, system_id, create_db=False):
    storage = PostgresStorage(system_id)
    storage.connect(settings['base_connection'], create_db)
    sync.init(storage)


def init_mongo(settings, system_id, create_db=False):
    storage = MongoStorage(system_id)
    storage.connect(settings['base_connection'], create_db)
    sync.init(storage)


class Storage(object):
    """Abstract class that defines a public interface for all storage
    implementations.

    """

    def connect(self, _=None, __=None):
        """Setup a connection to the storage backend if needed."""
        raise NotImplementedError

    def disconnect(self):
        """Destroy the connection to the storage backend if needed."""
        raise NotImplementedError

    def drop(self):
        """Delete all data for the current sync system."""
        raise NotImplementedError

    def start_transaction(self):
        """If the backend supports transactions start a new transaction."""
        raise NotImplementedError

    def commit(self):
        """If the backend supports transactions, commit the current
        transaction.

        """
        raise NotImplementedError

    def rollback(self):
        """If the backend supports transactions, roll back the current
        transaction.

        """
        raise NotImplementedError

    def save_system(self, system):
        """Save a system object to the storage backend.

        :param system: System object to save
        :type system: sync.System
        """
        raise NotImplementedError

    def save_node(self, node):
        """Save a node object to the storage backend.

        :param node: Node object to save
        :type node: sync.Node
        """
        raise NotImplementedError

    def save_message(self, message):
        """Save a message object to the storage backend.

        :param message: Message object to save
        :type message: sync.Message
        """
        raise NotImplementedError

    def save_error(self, error):
        """Save a error object to the storage backend.

        :param error: Error object to save
        :type error: sync.Error
        """
        raise NotImplementedError

    def save_change(self, change):
        """Save a change object to the storage backend.

        :param error: Change object to save
        :type error: sync.Change
        """
        raise NotImplementedError

    def save_record(self, record):
        """Save a record object to the storage backend.

        :param error: Record object to save
        :type error: sync.Record
        """
        raise NotImplementedError

    def save_remote(self, remote):
        """Save a remote object to the storage backend.

        :param remote: Remote object to save
        :type remote: sync.Remote
        """
        raise NotImplementedError

    def get_system(self):
        """Fetch the current sync systems system.

        :returns: Sync object
        :rtype: sync.System

        """
        raise NotImplementedError

    def get_node(self, node_id):
        """Fetch a node by its id.

        :param node_id: The id of the node to fetch.
        :returns: Node object.
        :rtype: sync.Node

        """
        raise NotImplementedError

    def get_record(self, record_id):
        """Fetch a record by it's id.

        :param record_id: The id of the record to fetch.
        :returns: A record object.
        :rtype: sync.Record

        """
        raise NotImplementedError

    def get_remote(self, node_id, remote_id=None, record_id=None):
        """Fetch a remote using a node id and either a remote or record id.

        :param node_id: The node id of the remote to fetch.
        :param remote_id: The remote_id of the remote to fetch.
        :param record_id: The record_id of the remote to fetch.
        :returns: A remote object
        :rtype: sync.Remote

        """
        raise NotImplementedError

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.constants.State.Pending,
                    with_for_update=False):
        """Fetch a message object based on the keyword args.

        :param message_id: The id of the message.
        :param destination_id: The destination node id of the message.
        :param state: The current state of the node.
        :param with_for_update: True if the storage backend should
            lock the row.
        :returns: A message object.
        :rtype: sync.Message

        """
        raise NotImplementedError

    def get_nodes(self):
        """Fetch all node objects in the system.

        :returns: An array of nodes.
        :rtype: array

        """
        raise NotImplementedError

    def get_records(self):
        """Fetch all records in the system.

        :returns: An iterator of record objects.
        :rtype: iterator

        """
        raise NotImplementedError

    def get_errors(self, message_id):
        """Fetch errors for a particular message.

        :param message_id: The id of the message the errors belong to.
        :returns: An array of errors.
        :rtype: array

        """
        raise NotImplementedError

    def get_changes(self, message_id):
        """Fetch changes for a particular message.

        :param message_id: The id of the message the changes belong to.
        :returns: An array of changes.
        :rtype: array

        """
        raise NotImplementedError

    def update_messages(self, node_id, record_id, remote_id):
        """Update the remote id of all messages with a matching node id and
        record id.

        :param node_id: The node id to filter by.
        :param record_id: The record id to filter by.
        :param remote_id: The remote id value to set.

        """
        raise NotImplementedError


class MockStorage(Storage):
    """Store data in-memory."""

    def __init__(self, system_id):
        self.id = system_id

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

    def connect(self, _=None, __=None):
        pass

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

    def save_system(self, system):
        if system.id is None:
            system.id = self.id
        self.system = copy.deepcopy(system)

    def save_node(self, node):
        self._save(node, self.nodes)

    def save_message(self, message):
        self._save(message, self.messages)

    def save_error(self, error):
        self._save(error, self.errors)

    def save_change(self, change):
        self._save(change, self.changes)

    def save_record(self, record):
        self._save(record, self.records)

    def save_remote(self, remote):
        self._save(remote, self.remotes)

    def get_system(self):
        if not hasattr(self, 'system'):
            return None
        return self.system

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

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.constants.State.Pending, with_for_update=False):
        if message_id is not None:
            return self.messages.get(message_id, None)

        if destination_id is not None:
            for message in self.messages.values():
                if message.state == state and \
                   message.destination_id == destination_id:
                    return message

        return None

    def get_nodes(self):
        return self.nodes.values()

    def get_records(self):
        results = []
        for r in self.records.values():
            if not r.deleted:
                results.append(r)
        return [results]

    def get_errors(self, message_id):
        results = []
        for e in self.errors.values():
            if message_id == e.message_id:
                results.append(e)
        return results

    def get_changes(self, message_id):
        results = []
        for c in self.changes.values():
            if message_id == c.message_id:
                results.append(c)
        return results

    def update_messages(self, node_id, record_id, remote_id):
        for message in self.messages.values():
            if message.state == sync.constants.State.Pending \
               and message.destination_id == node_id \
               and message.record_id == record_id:
                message.remote_id = remote_id


class PostgresStorage(Storage):
    """Store data in a Postgres database using SqlAlchemy."""

    def __init__(self, system_id):
        self.id = system_id
        self.base_url = None
        self.engine = None
        self.connection = None
        self.trans = []

    def _get_one(self, query, class_, with_for_update=False):
        if with_for_update:
            query = query.with_for_update()

        results = self.connection.execute(query)
        record = results.fetchone()

        if record is None:
            return None

        obj = class_()
        for key in record.keys():
            setattr(obj, key, record[key])

        return obj

    def _get_many(self, query, class_):
        rows = self.connection.execute(query)
        rows = rows.fetchall()

        if rows is None:
            return []

        results = []
        for row in rows:
            obj = class_()
            for key in row.keys():
                setattr(obj, key, row[key])
            results.append(obj)

        return results

    def _save(self, table, obj, override_id=False):
        values = obj.as_dict(False)
        keep = {}
        for key in values.keys():
            if key in table.columns.keys():
                keep[key] = values[key]
        values = keep

        if not hasattr(obj, 'id') or obj.id is None:
            obj.id = override_id or sync.generate_id()
            values['id'] = obj.id
            op = sqla.insert(table)
            op = op.values(values)
        else:
            op = sqla.update(table)
            op = op.values(values)
            op = op.where(table.c.id == obj.id)

        self.connection.execute(op)

    def _connect(self):
        self.connection = self.engine.connect()
        self.metadata = sqla.MetaData(bind=self.engine)

    def _setup_schema(self, create_db=False):
        self.system_table = sqla.Table(
            "systems", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "name",
                sqla.types.String,
                nullable=True),
            sqla.Column(
                "fetch_before_send",
                sqla.types.Boolean,
                default=True,
                nullable=False),
            sqla.Column(
                "schema",
                postgresql.JSON,
                nullable=False))

        self.node_table = sqla.Table(
            "nodes", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "name",
                sqla.types.String,
                nullable=True),
            sqla.Column(
                "create",
                sqla.types.Boolean,
                default=True,
                nullable=False),
            sqla.Column(
                "read",
                sqla.types.Boolean,
                default=True,
                nullable=False),
            sqla.Column(
                "update",
                sqla.types.Boolean,
                default=True,
                nullable=False),
            sqla.Column(
                "delete",
                sqla.types.Boolean,
                default=True,
                nullable=False))

        self.message_table = sqla.Table(
            "messages", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "parent_id",
                postgresql.UUID,
                sqla.ForeignKey("messages.id"),
                nullable=True),
            sqla.Column(
                "origin_id",
                postgresql.UUID,
                sqla.ForeignKey("nodes.id"),
                nullable=True),
            sqla.Column(
                "destination_id",
                postgresql.UUID,
                sqla.ForeignKey("nodes.id"),
                nullable=True),
            sqla.Column(
                "timestamp",
                sqla.DateTime,
                nullable=False),
            sqla.Column(
                "method",
                sqla.types.String,
                nullable=False),
            sqla.Column(
                "payload",
                postgresql.JSON,
                nullable=True),
            sqla.Column(
                "remote_id",
                sqla.types.String,
                nullable=True),
            sqla.Column(
                "record_id",
                sqla.types.String,
                nullable=True),
            sqla.Column(
                "state",
                sqla.types.String,
                nullable=False))

        self.error_table = sqla.Table(
            "errors", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "message_id",
                postgresql.UUID,
                sqla.ForeignKey("messages.id"),
                nullable=False),
            sqla.Column(
                "timestamp",
                sqla.DateTime,
                nullable=False),
            sqla.Column(
                "text",
                sqla.Text,
                nullable=True))

        self.change_table = sqla.Table(
            "changes", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "message_id",
                postgresql.UUID,
                sqla.ForeignKey("messages.id"),
                nullable=False),
            sqla.Column(
                "timestamp",
                sqla.DateTime,
                nullable=False),
            sqla.Column(
                "state",
                sqla.types.String,
                nullable=False),
            sqla.Column(
                "note",
                sqla.Text,
                nullable=True))

        self.record_table = sqla.Table(
            "records", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "last_updated",
                sqla.DateTime,
                nullable=False),
            sqla.Column(
                "deleted",
                sqla.types.Boolean,
                default=True,
                nullable=False),
            sqla.Column(
                "head",
                postgresql.JSON,
                nullable=True))

        self.remote_table = sqla.Table(
            "remotes", self.metadata,
            sqla.Column(
                "id",
                postgresql.UUID,
                primary_key=True),
            sqla.Column(
                "node_id",
                postgresql.UUID,
                sqla.ForeignKey("nodes.id"),
                nullable=True),
            sqla.Column(
                "remote_id",
                sqla.types.String,
                nullable=False),
            sqla.Column(
                "record_id",
                postgresql.UUID,
                sqla.ForeignKey("records.id"),
                nullable=True))

        if create_db:
            self.metadata.create_all()

    def connect(self, base_url, create_db=False):
        self.base_url = base_url
        self.engine = sqla.create_engine(self.base_url + self.id)

        if not database_exists(self.engine.url) and not create_db:
            raise DatabaseNotFoundError()

        if not database_exists(self.engine.url):
            create_database(self.engine.url)

        self._connect()
        self._setup_schema(create_db)

    def disconnect(self):
        self.connection.close()

    def drop(self):
        drop_database(self.engine.url)

    def start_transaction(self):
        tran = self.connection.begin()
        self.trans.append(tran)

    def commit(self):
        tran = self.trans.pop(-1)
        tran.commit()

    def rollback(self):
        tran = self.trans.pop(-1)
        tran.rollback()

    def save_system(self, system):
        self._save(self.system_table, system, self.id)

    def save_node(self, node):
        self._save(self.node_table, node)

    def save_message(self, message):
        self._save(self.message_table, message)

    def save_error(self, error):
        self._save(self.error_table, error)

    def save_change(self, change):
        self._save(self.change_table, change)

    def save_record(self, record):
        self._save(self.record_table, record)

    def save_remote(self, remote):
        self._save(self.remote_table, remote)

    def get_system(self):
        query = sqla.select([self.system_table])

        return self._get_one(query, sync.System)

    def get_node(self, node_id):
        table = self.node_table
        query = table.select()
        query = query.where(table.c.id == node_id)

        return self._get_one(query, sync.Node)

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.constants.State.Pending,
                    with_for_update=False):
        table = self.message_table
        query = table.select()

        if message_id is not None:
            query = query.where(table.c.id == message_id)
        elif destination_id is not None:
            query = query.where(sqla.and_(
                table.c.state == state,
                table.c.destination_id == destination_id))

        return self._get_one(query, sync.Message, with_for_update)

    def get_record(self, record_id):
        table = self.record_table
        query = table.select()
        query = query.where(table.c.id == record_id)

        record = self._get_one(query, sync.Record)

        if record is None:
            return None

        record.remotes = self.get_remotes([record_id])

        return record

    def get_remote(self, node_id, remote_id=None, record_id=None):
        if remote_id is None and record_id is None:
            raise sync.exceptions.InvalidOperationError(
                Text.RemoteOrRecordRequired)

        table = self.remote_table
        query = table.select()
        query = query.where(table.c.node_id == node_id)

        if remote_id is not None:
            query = query.where(table.c.remote_id == remote_id)
        elif record_id is not None:
            query = query.where(table.c.record_id == record_id)

        return self._get_one(query, sync.Remote)

    def get_remotes(self, record_ids):
        table = self.remote_table
        query = table.select()
        query = query.where(table.c.record_id.in_(record_ids))

        return self._get_many(query, sync.Remote)

    def get_nodes(self):
        table = self.node_table
        query = table.select()

        return self._get_many(query, sync.Node)

    def get_records(self):
        """Fetch all records using a generator.

        This is more complicated than usual for performance. It
        streams batches of records (1000) at a time.

        :returns: Batches of records.
        :rtype: generator

        """
        connection = None
        try:
            # Create a second connection to Postgres so yield_per and
            # committing transactions do not interfere with each
            # other.
            engine = sqla.create_engine(self.engine.url)
            connection = engine.connect()

            # Setup the basic select statement.
            table = self.record_table
            query = table.select()
            query = query.where(table.c.deleted == False)  # noqa

            # Query with stream_results set to True.
            result = (connection.execution_options(stream_results=True)
                      .execute(query))

            while True:
                # Use a dictionary so that the associated remote objects
                # can easily be added into the appropriate
                # 'record.remotes' object cache.
                results = {}

                # Fetch a batch of records.
                chunk = result.fetchmany(1000)
                if not chunk:
                    break

                # Inflate the record objects.
                for row in chunk:
                    obj = sync.Record()
                    for key in row.keys():
                        setattr(obj, key, row[key])
                    results[obj.id] = obj

                # Efficiently fetch the associated remote objects for
                # this batch of records.
                remotes = self.get_remotes(results.keys())

                # Add the remote objects to the associated record.remotes
                # cache.
                for remote in remotes:
                    results[remote.record_id].remotes.append(remote)

                yield results.values()
        finally:
            if connection is not None:
                connection.close()

    def get_errors(self, message_id):
        table = self.error_table
        query = table.select()
        query = query.where(table.c.message_id == message_id)

        return self._get_many(query, sync.Error)

    def get_changes(self, message_id):
        table = self.change_table
        query = table.select()
        query = query.where(table.c.message_id == message_id)

        return self._get_many(query, sync.Change)

    def update_messages(self, node_id, record_id, remote_id):
        table = self.message_table
        op = sqla.update(table)
        op = op.values({'remote_id': remote_id})
        op = op.where(sqla.and_(
            table.c.destination_id == node_id,
            table.c.record_id == record_id))
        self.connection.execute(op)


class MongoStorage(Storage):
    """Store data in a Mongo database."""

    def __init__(self, system_id):
        self.id = system_id
        self.session = None

    def _get_one(self, table, filter_, class_):
        record = self.session[table].find_one(filter_)

        if record is None:
            return None

        obj = class_()
        for key in record.keys():
            if not key == '_id':
                setattr(obj, key, record[key])

        return obj

    def _get_many(self, table, filter_, class_):
        rows = self.session[table].find(filter_)

        if rows is None:
            return []

        results = []
        for row in rows:
            obj = class_()
            for key in row.keys():
                if not key == '_id':
                    setattr(obj, key, row[key])
            results.append(obj)

        return results

    def _save(self, table, obj, override_id=False):
        values = obj.as_dict(False)
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
        if self.id in names:
            return True
        else:
            return False

    def connect(self, base_url, create_db=False, client=None):
        self.base_url = base_url
        if client is None:
            self.client = MongoClient(base_url)
        else:
            self.client = client

        if not self._database_exists() and not create_db:
            raise DatabaseNotFoundError()

        self.session = self.client[self.id]

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

    def save_system(self, system):
        self._save('systems', system, self.id)

    def save_node(self, node):
        self._save('nodes', node)

    def save_message(self, message):
        self._save('messages', message)

    def save_error(self, error):
        self._save('errors', error)

    def save_change(self, change):
        self._save('changes', change)

    def save_record(self, record):
        self._save('records', record)

    def save_remote(self, remote):
        self._save('remotes', remote)

    def get_system(self):
        return self._get_one('systems', {}, sync.System)

    def get_node(self, node_id):
        filter_ = {
            'id': node_id
        }
        return self._get_one('nodes', filter_, sync.Node)

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.constants.State.Pending,
                    with_for_update=False):
        filter_ = {}

        if message_id is not None:
            filter_['id'] = message_id
        elif destination_id is not None:
            filter_['state'] = state
            filter_['destination_id'] = destination_id

        return self._get_one('messages', filter_, sync.Message)

    def get_record(self, record_id):
        filter_ = {
            'id': record_id
        }

        record = self._get_one('records', filter_, sync.Record)

        if record is None:
            return None

        record.remotes = self.get_remotes([record_id])

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
            'record_id': record_ids
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
        chunk = self._get_many('records', filter_, sync.Record)

        while True:
            # Use a dictionary so that the associated remote objects
            # can easily be added into the appropriate
            # 'record.remotes' object cache.
            results = {}

            # Fetch a batch of records.
            if not chunk:
                break

            for obj in chunk:
                results[obj.id] = obj

            chunk = None

            # Efficiently fetch the associated remote objects for
            # this batch of records.
            remotes = self.get_remotes(results.keys())

            # Add the remote objects to the associated record.remotes
            # cache.
            for remote in remotes:
                results[remote.record_id].remotes.append(remote)

            yield results.values()

    def get_errors(self, message_id):
        filter_ = {
            'message_id': message_id
        }
        return self._get_many('errors', filter_, sync.Error)

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
        values = {'remote_id': remote_id}
        self.session['messages'].update(filter_, values)
