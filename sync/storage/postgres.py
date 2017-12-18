import sqlalchemy as sqla

from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
from sqlalchemy_utils import create_database, drop_database

import sync

from sync import settings
from sync import Text
from sync.exceptions import DatabaseNotFoundError
from sync.storage.base import Storage


class PostgresStorage(Storage):
    """Store data in a Postgres database using SqlAlchemy."""

    def __init__(self, network_id):
        self.id = network_id
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
        self.network_table = sqla.Table(
            "networks", self.metadata,
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

    def connect(self, create_db=False):
        self.base_url = settings.POSTGRES_CONNECTION
        self.engine = sqla.create_engine(self.base_url + self.id)

        try:
            self._connect()
        except OperationalError as e:
            # sqlalchemy_utils supplies a database_exists() function
            # but it is much more performant for the common uses of
            # the API to assume the database exists and catch the
            # exception where it doesn't. Unfortunately, psycopg2
            # doesn't supply error codes before the connection has
            # been established.
            if 'does not exist' not in str(e):
                raise

            if not create_db:
                raise DatabaseNotFoundError()

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

    def save_network(self, network):
        self._save(self.network_table, network, self.id)

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

    def get_network(self):
        query = sqla.select([self.network_table])

        return self._get_one(query, sync.Network)

    def get_node(self, node_id):
        table = self.node_table
        query = table.select()
        query = query.where(table.c.id == node_id)

        return self._get_one(query, sync.Node)

    def get_message(self, message_id=None, destination_id=None,
                    state=sync.State.Pending,
                    with_for_update=False):
        table = self.message_table
        query = table.select()

        if message_id is not None:
            query = query.where(table.c.id == message_id)
        elif destination_id is not None:
            query = query.where(sqla.and_(
                table.c.state == state,
                table.c.destination_id == destination_id))

        query = query.order_by(table.c.timestamp)

        return self._get_one(query, sync.Message, with_for_update)

    def get_message_count(self, destination_id=None, state=sync.State.Pending):
        table = self.message_table
        query = table.select()

        query = query.where(sqla.and_(
            table.c.state == state,
            table.c.destination_id == destination_id))

        rows = self.connection.execute(query)
        return rows.rowcount

    def get_record(self, record_id):
        table = self.record_table
        query = table.select()
        query = query.where(table.c.id == record_id)

        record = self._get_one(query, sync.Record)

        if record is None:
            return None

        record._remotes = self.get_remotes([record_id])

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
                    results[remote.record_id]._remotes.append(remote)

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
