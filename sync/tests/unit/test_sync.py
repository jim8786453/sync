import json
import jsonschema
import mongomock
import os
import os.path
import pytest
import sqlalchemy

from operator import itemgetter

import sync

from sync import exceptions, storage, tasks
from sync.core import merge_patch
from sync.conftest import postgresql
from sync.storage import Storage


class MockError(Exception):
    pass


def error_fun(_, __=None):
    raise MockError('Mock')


def test_schema():
    """"Returns a JSON schema document that validates the data returned by
    test_data().

    """
    path = os.path.dirname(os.path.abspath(__file__))
    json_file = path + '/test_schema.json'
    with open(json_file) as schema_file:
        schema = json.load(schema_file)
    assert schema is not None
    return schema


def test_data():
    """"Returns a JSON data taken from an open data repository. The
    original data can be found at the following Github repository:

    https://github.com/tategallery/collection

    """
    path = os.path.dirname(os.path.abspath(__file__))
    json_file = path + '/test_data.json'
    with open(json_file) as data_file:
        data = json.load(data_file)
    assert len(data) == 99
    return data


def generate_mock_storage():
    """Return a sync.storage.MockStorage object.

    """
    mock_storage = storage.MockStorage(sync.generate_id())
    mock_storage.connect(create_db=True)
    return mock_storage


def generate_postgresql_storage():
    """Return a sync.storage.PostgresStorage object.

    """
    sync.settings.POSTGRES_CONNECTION = postgresql.url()
    postgres_storage = storage.PostgresStorage(sync.generate_id())
    postgres_storage.connect(create_db=True)
    return postgres_storage


def generate_mongo_storage():
    """Return a sync.storage.PostgresStorage object.

    """
    sync.storage.mongo.test_mongo_client = mongomock.MongoClient()
    mongo_storage = storage.MongoStorage(sync.generate_id())
    mongo_storage.connect(create_db=True)
    return mongo_storage


STORAGE_GENERATORS = [
    generate_mock_storage,
    generate_postgresql_storage,
    generate_mongo_storage
]


def test_close_none_storage():
    # Closing before init should not raise an error.
    sync.close()
    assert True


def test_invalid_postgres_connect():
    sync.settings.POSTGRES_CONNECTION = 'postgresql://foo:bar@localhost:1234/'
    postgres_storage = storage.PostgresStorage(sync.generate_id())
    with pytest.raises(sqlalchemy.exc.OperationalError) as excinfo:
        postgres_storage.connect()


@pytest.mark.noautouse
def test_tasks_call_close():
    tasks._call_close()
    sync.settings.STORAGE_CLASS = 'MockStorage'
    tasks._call_close()


@pytest.mark.parametrize('storage_fun', STORAGE_GENERATORS)
class TestSync():

    @pytest.fixture(autouse=True)
    def storage(self, request, session_setup, storage_fun):
        self.storage = storage_fun()

        sync.init(self.storage)
        sync.Network.init('test', {}, True)

        assert sync.current_storage() == self.storage

        yield

        sync.current_storage().drop()

    @pytest.fixture(autouse=False)
    def test_schema(self, request):
        network = sync.Network.get()
        network.schema = test_schema()
        network.save()
        self.data = test_data()

    def test_misc_storage_get_remote(self):
        with pytest.raises(exceptions.InvalidOperationError):
            sync.current_storage().get_remote('', None, None)

    def test_network(self):
        network = sync.Network.get()
        network.name = 'Mock'
        network.schema = {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                }
            }
        }
        network.save()
        returned = sync.Network.get()
        assert network.id is not None
        assert network == returned

        network.name = 'Update'
        network.save()
        assert network != returned
        returned = sync.Network.get()
        assert network == returned

        network.schema = None
        network.save()
        assert network != returned
        returned = sync.Network.get()
        assert network == returned

    def test_node(self):
        node = sync.Node.create('Mock', create=True, read=True,
                                update=True, delete=True)
        node.save()
        returned = sync.Node.get(node.id)
        assert node.id is not None
        assert node == returned

        node.name = 'Update'
        node.read = True
        assert node != returned
        node.save()
        returned = sync.Node.get(node.id)
        assert node == returned

    def test_message(self):
        node = sync.Node()
        node.save()

        message = sync.Message()
        message.origin_id = node.id
        message.method = sync.Method.Create
        message.payload = {}
        message.save()
        returned = sync.Message.get(message.id)
        assert message.id is not None
        assert message == returned

        message.state = sync.State.Processing
        assert message != returned
        message.save()
        returned = sync.Message.get(message.id)
        assert message == returned

    def test_change(self):
        message = sync.Message()
        message.method = sync.Method.Create
        message.save()

        change = sync.Change()
        change.message_id = message.id
        change.state = sync.State.Processing
        change.note = "mock"
        change.save()
        returned = message.changes()

        assert len(returned) == 1
        assert returned[0] == change

        with pytest.raises(Exception):
            change.save()

    def test_record(self):
        record = sync.Record()
        record.deleted = True
        record.head = {'name': 'mock'}
        record.save()
        returned = sync.Record.get(record.id)
        assert record.id is not None
        assert record == returned

        record.deleted = False
        assert record != returned
        record.save()
        returned = sync.Record.get(record.id)
        assert record == returned

    def test_remote(self):
        node = sync.Node()
        node.save()

        record = sync.Record()
        record.deleted = True
        record.head = {'name': 'mock'}
        record.save()

        remote = sync.Remote()
        remote.node_id = node.id
        remote.remote_id = 'id'
        remote.record_id = record.id
        remote.save()

        returned = sync.Remote.get(node.id, remote_id='id')
        assert remote.id is not None
        assert remote == returned
        assert record.id == returned.record_id

        returned = sync.Remote.get(node.id, record_id=record.id)
        assert remote.id is not None
        assert remote == returned
        assert record.id == returned.record_id

        remote.node_id = None
        remote.save()
        assert remote != returned
        assert sync.Remote.get(node.id, remote_id='id') is None
        assert sync.Remote.get(node.id, record_id=record.id) is None

    def test_generate_id(self):
        first = sync.generate_id()
        second = sync.generate_id()
        assert first != second

    def test_merge_patch(self):
        original = {"a": "b"}
        patch = {"a": "c"}
        result = {"a": "c"}
        assert result == merge_patch(original, patch)

        original = {"a": "b"}
        patch = {"b": "c"}
        result = {"a": "b", "b": "c"}
        assert result == merge_patch(original, patch)

        original = {"a": "b"}
        patch = {"a": None}
        result = {}
        assert result == merge_patch(original, patch)

        original = {"a": "b", "b": "c"}
        patch = {"a": None}
        result = {"b": "c"}
        assert result == merge_patch(original, patch)

        original = {"a": ["b"]}
        patch = {"a": "c"}
        result = {"a": "c"}
        assert result == merge_patch(original, patch)

        original = {"a": "c"}
        patch = {"a": ["b"]}
        result = {"a": ["b"]}
        assert result == merge_patch(original, patch)

        original = {"a": [{"b": "c"}]}
        patch = {"a": [1]}
        result = {"a": [1]}
        assert result == merge_patch(original, patch)

        original = ["a", "b"]
        patch = ["c", "d"]
        result = ["c", "d"]
        assert result == merge_patch(original, patch)

        original = {"a": "b"}
        patch = ["c"]
        result = ["c"]
        assert result == merge_patch(original, patch)

        original = {"a": "foo"}
        patch = None
        result = None
        assert result == merge_patch(original, patch)

        original = {"a": "foo"}
        patch = "bar"
        result = "bar"
        assert result == merge_patch(original, patch)

        original = {"e": None}
        patch = {"a": 1}
        result = {"e": None, "a": 1}
        assert result == merge_patch(original, patch)

        original = [1, 2]
        patch = {"a": "b", "c": None}
        result = {"a": "b"}
        assert result == merge_patch(original, patch)

        original = {}
        patch = {"a": {"bb": {"ccc": None}}}
        result = {"a": {"bb": {}}}
        assert result == merge_patch(original, patch)

    def test_base(self):
        em_1 = sync.Base()
        em_1.prop_1 = 'test'
        em_1.prop_2 = 2

        em_2 = sync.Base()
        em_2.prop_1 = 'test'
        em_2.prop_2 = 2

        assert em_1 == em_2
        em_2.prop_2 = 0
        assert em_1 != em_2

    def test_node_send(self):
        node = sync.Node.create(create=True, read=True, update=True,
                                delete=True)
        method = sync.Method.Create
        payload = {'foo': 'bar'}
        message = node.send(method, payload)

        message = sync.Message.get(message.id)

        assert message.method == method
        assert message.payload == payload
        assert message.record_id is not None
        assert message.state == sync.State.Acknowledged

    def test_sync_node_has_pending(self):
        n1 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        n2 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        assert n2.has_pending() == 0
        n1.send(sync.Method.Create, {'foo': 'bar'})
        assert n2.has_pending() == 1
        n2.fetch()
        assert n2.has_pending() == 0

    def test_node_fetch_ack(self):
        sender = sync.Node.create(create=True)
        fetcher = sync.Node.create(read=True)

        method = sync.Method.Create
        payload = {'foo': 'bar'}

        sent = sender.send(method, payload)
        fetched = fetcher.fetch()

        # Only a single message should be available.
        assert fetcher.fetch() is None

        assert sent.parent_id is None
        assert sent.origin_id == sender.id
        assert sent.destination_id is None
        assert sent.id == fetched.parent_id

        assert fetched.origin_id is None
        assert fetched.destination_id == fetcher.id
        assert fetched.state == sync.State.Processing

        message = fetcher.acknowledge(fetched.id)
        assert message.state == sync.State.Acknowledged

    def test_node_fetch_ack_with_remote_returns_remote(self):
        sender = sync.Node.create(create=True)
        fetcher = sync.Node.create(read=True)
        method = sync.Method.Create
        payload = {'foo': 'bar'}
        sender.send(method, payload)
        fetched = fetcher.fetch()
        message = fetcher.acknowledge(fetched.id, 'foo')
        assert message.remote_id is not None

    def test_node_fetch_ack_another_nodes_message(self):
        sender = sync.Node.create(create=True)
        fetcher = sync.Node.create(read=True)

        method = sync.Method.Create
        payload = {'foo': 'bar'}

        sender.send(method, payload)
        fetched = fetcher.fetch()

        # Try and ack a message for another node.
        with pytest.raises(sync.exceptions.InvalidOperationError):
            sender.acknowledge(fetched.id)

    def test_node_fetch_ack_invalid_message(self):
        sender = sync.Node.create(create=True)

        # Try and ack a message for another node.
        with pytest.raises(sync.exceptions.InvalidOperationError):
            sender.acknowledge('foo')

    def test_node_fetch_ack_unknown_message(self):
        sender = sync.Node.create(create=True)

        # Try and ack a message for another node.
        with pytest.raises(sync.exceptions.NotFoundError):
            sender.acknowledge(sync.generate_id())

    def test_node_fetch_fail_another_nodes_message(self):
        sender = sync.Node.create(create=True)
        fetcher = sync.Node.create(read=True)

        method = sync.Method.Create
        payload = {'foo': 'bar'}

        sender.send(method, payload)
        fetched = fetcher.fetch()

        # Try and ack a message for another node.
        with pytest.raises(sync.exceptions.InvalidOperationError):
            sender.fail(fetched.id)

    def test_node_fetch_fail(self):
        sender = sync.Node.create(create=True)
        fetcher = sync.Node.create(read=True)

        method = sync.Method.Create
        payload = {'foo': 'bar'}

        sent = sender.send(method, payload)
        fetched = fetcher.fetch()

        # Only a single message should be available.
        assert fetcher.fetch() is None

        assert sent.parent_id is None
        assert sent.origin_id == sender.id
        assert sent.destination_id is None
        assert sent.id == fetched.parent_id

        assert fetched.origin_id is None
        assert fetched.destination_id == fetcher.id
        assert fetched.state == sync.State.Processing

        message = fetcher.fail(fetched.id)
        assert message.state == sync.State.Failed

    def test_node_sync(self):
        sender = sync.Node.create(create=True)

        method = sync.Method.Create
        payload = {'foo': 'bar'}

        sender.send(method, payload)
        sender.send(method, payload)
        sender.send(method, payload)

        fetcher = sync.Node.create(read=True)
        assert fetcher.fetch() is None

        fetcher.sync()

        for i in range(3):
            assert fetcher.fetch() is not None

        assert fetcher.fetch() is None

    def test_node_check(self):
        node = sync.Node()

        assert not node.check(sync.Method.Create)
        assert not node.check(sync.Method.Read)
        assert not node.check(sync.Method.Update)
        assert not node.check(sync.Method.Delete)
        assert not node.check(None)

        node.save()
        node = sync.Node.get(node.id)

        assert not node.check(sync.Method.Create)
        assert not node.check(sync.Method.Read)
        assert not node.check(sync.Method.Update)
        assert not node.check(sync.Method.Delete)
        assert not node.check(None)

        node.create = True
        node.read = True
        node.update = True
        node.delete = True

        assert node.check(sync.Method.Create)
        assert node.check(sync.Method.Read)
        assert node.check(sync.Method.Update)
        assert node.check(sync.Method.Delete)

        node.save()
        node = sync.Node.get(node.id)

        assert node.check(sync.Method.Create)
        assert node.check(sync.Method.Read)
        assert node.check(sync.Method.Update)
        assert node.check(sync.Method.Delete)

        node.update = False

        assert node.check(sync.Method.Create)
        assert node.check(sync.Method.Read)
        assert not node.check(sync.Method.Update)
        assert node.check(sync.Method.Delete)

        node.save()
        node = sync.Node.get(node.id)

        assert node.check(sync.Method.Create)
        assert node.check(sync.Method.Read)
        assert not node.check(sync.Method.Update)
        assert node.check(sync.Method.Delete)

    def test_node_disable(self):
        node = sync.Node()

        node.create = True
        node.read = True
        node.update = True
        node.delete = True

        node.save()
        node = sync.Node.get(node.id)

        assert node.create is True
        assert node.read is True
        assert node.update is True
        assert node.delete is True

        node.disable()
        node = sync.Node.get(node.id)

        assert node.create is False
        assert node.read is False
        assert node.update is False
        assert node.delete is False

    def test_message_execute(self):
        node = sync.Node.create(create=True)
        payload = {'foo': 'bar'}

        message = sync.Message()
        message.origin_id = node.id
        message.method = sync.Method.Create
        message.payload = payload
        message._execute()

        assert message._record is not None
        assert message._record.head == payload

        message.method = sync.Method.Delete
        message._execute()

        assert message._record is not None
        assert message._record.head is None
        assert message._record.deleted

    def test_message_inflate(self):
        node = sync.Node.create(create=True)

        parent = sync.Message()
        parent.save()

        record = sync.Record()
        record.deleted = True
        record.head = {'name': 'mock'}
        record.save()

        message = sync.Message()
        message.parent_id = parent.id
        message.origin_id = node.id
        message.destination_id = node.id
        message.record_id = record.id
        message._inflate()

        assert message._parent == parent
        assert message._origin == node
        assert message._destination == node
        assert message._record == record

        remote = sync.Remote()
        remote.node_id = node.id
        remote.remote_id = 'id'
        remote.record_id = record.id
        remote.save()

        message.remote_id = 'id'
        message._record = None
        message._record_id = None
        message._inflate()

        assert message._record is not None
        assert message._record.id == record.id

    def test_message_propagate(self):
        node_1 = sync.Node.create(create=True)
        node_2 = sync.Node.create(read=True)
        node_3 = sync.Node.create(read=True)
        node_4 = sync.Node.create(read=True)
        node_5 = sync.Node.create()

        payload = {'foo': 'bar'}

        message = sync.Message()
        message.origin_id = node_1.id
        message.method = sync.Method.Create
        message.payload = payload
        message._execute()

        remote = sync.Remote()
        remote.node_id = node_2.id
        remote.remote_id = 'id'
        remote.record_id = message.record_id
        remote.save()

        message._propagate()

        assert node_1.fetch() is None

        fetched_with_remote = node_2.fetch()
        assert fetched_with_remote is not None
        assert fetched_with_remote.remote_id == 'id'

        assert node_3.fetch() is not None
        assert node_4.fetch() is not None
        assert node_5.fetch() is None

    def test_message_validate(self):
        message = sync.Message()
        message.parent_id = 'foo'
        with pytest.raises(sync.exceptions.NotFoundError):
            message._validate()

        message = sync.Message()
        message.origin_id = 'foo'
        with pytest.raises(sync.exceptions.NotFoundError):
            message._validate()

        message = sync.Message()
        message.destination_id = 'foo'
        with pytest.raises(sync.exceptions.NotFoundError):
            message._validate()

        message = sync.Message()
        message.method = sync.Method.Create
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message._validate()

        message = sync.Message()
        message.method = sync.Method.Delete
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message._validate()

        node_1 = sync.Node.create(create=True, read=True)
        node_2 = sync.Node.create(create=True, read=True)
        node_1.send(sync.Method.Create, {})
        network = sync.Network.get()
        network.fetch_before_send = True
        message = sync.Message()
        message.method = sync.Method.Create
        message.payload = {}
        message.origin_id = node_2.id
        message._origin = node_2
        message._network = network
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message._validate()

        message = sync.Message()
        message.method = sync.Method.Create
        node = sync.Node.create()
        message = sync.Message()
        message.method = sync.Method.Create
        message.payload = {}
        message.origin_id = node.id
        message._origin = node
        message._network = network
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message._validate()

    def test_message_validate_deny_update_deleted_record(self):
        node_1 = sync.Node.create(create=True, read=True, update=True,
                                  delete=True)
        message = node_1.send(sync.Method.Create, {})
        node_1.send(sync.Method.Delete, record_id=message.record_id)
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message = node_1.send(sync.Method.Update, {},
                                  record_id=message.record_id)

    def test_message_update(self):
        message = sync.Message()

        # Can not update a message until it has been saved.
        with pytest.raises(AssertionError):
            message.update(sync.State.Processing)

        message.save()
        message.update(sync.State.Processing)
        message.update(sync.State.Pending)

        another = sync.Message()
        another.save()
        another.update(sync.State.Processing)

        assert len(message.changes()) == 2

        with pytest.raises(sync.exceptions.InvalidOperationError):
            message.update(sync.State.Pending)

    def test_message_acknowledge(self):
        message = sync.Message()
        message.save()
        message.update(sync.State.Processing)
        message.acknowledge()
        assert message.state == sync.State.Acknowledged

    def test_message_acknowledge_with_remote(self):
        node = sync.Node.create(create=True)

        record = sync.Record()
        record.deleted = True
        record.head = {'name': 'mock'}
        record.save()

        message = sync.Message()
        message.save()
        message.update(sync.State.Processing)
        message.record_id = record.id
        message.destination_id = node.id
        message.acknowledge('remote_id')

        remote = sync.Remote.get(node.id, remote_id='remote_id')

        assert message.state == sync.State.Acknowledged
        assert remote.record_id == record.id

    def test_message_fail(self):
        message = sync.Message()
        message.save()
        message.update(sync.State.Processing)
        message.fail()
        assert message.state == sync.State.Failed

    def test_message_fail_with_reason(self):
        message = sync.Message()
        message.save()
        message.update(sync.State.Processing)
        message.fail("reason")
        assert message.state == sync.State.Failed
        changes = message.changes()
        assert changes[-1].note == "reason"

    def test_message_send(self):
        node_1 = sync.Node.create()

        with pytest.raises(sync.exceptions.InvalidOperationError):
            message = sync.Message.send(node_1.id,
                                        sync.Method.Create,
                                        payload={})

        node_1.create = True
        node_1.save()
        message = sync.Message.send(node_1.id, sync.Method.Create,
                                    payload={})
        assert message.state == sync.State.Acknowledged

    def test_message_fetch(self):
        node_1 = sync.Node.create(create=True)
        node_2 = sync.Node.create(read=True)

        sync.Message.send(node_1.id, sync.Method.Create,
                          payload={'foo': 'bar'})
        read = sync.Message.fetch(node_2.id)
        assert read is not None
        assert read.state == sync.State.Processing
        assert sync.Message.fetch(node_2.id) is None
        read.acknowledge()
        read = sync.Message.get(read.id)
        assert read.state == sync.State.Acknowledged

    def test_record_validate(self):
        record = sync.Record()

        network = sync.Network.get()
        network.schema = {'type': 'string'}
        network.save()

        record.head = 'I am a string'
        assert record.validate()

        record.head = 42
        with pytest.raises(jsonschema.exceptions.ValidationError):
            record.validate()

    def test_sync_single_write_multi_read(self):
        n1 = sync.Node.create(create=True, update=True, delete=True)
        n2 = sync.Node.create(read=True)
        n3 = sync.Node.create(read=True)
        n4 = sync.Node.create(read=True)

        read_nodes = [n2, n3, n4]

        # Create
        message = n1.send(sync.Method.Create, {'foo': 'bar'})
        assert message.state == sync.State.Acknowledged
        assert message.record_id is not None
        for node in read_nodes:
            assert node.fetch() is not None
            assert node.fetch() is None

        # Update
        message = n1.send(sync.Method.Update, {},
                          record_id=message.record_id)
        assert message.state == sync.State.Acknowledged
        for node in read_nodes:
            assert node.fetch() is not None
            assert node.fetch() is None

        # Delete
        message = n1.send(sync.Method.Delete,
                          record_id=message.record_id)
        assert message.state == sync.State.Acknowledged
        for node in read_nodes:
            assert node.fetch() is not None
            assert node.fetch() is None

        # Resync a new node, no records should exist.
        n5 = sync.Node.create(read=True)
        n5.sync()
        assert node.fetch() is None

    def test_sync_multi_write_single_read(self):
        n1 = sync.Node.create(create=True)
        n2 = sync.Node.create(create=True)
        n3 = sync.Node.create(create=True)
        n4 = sync.Node.create(read=True)

        write_nodes = [n1, n2, n3]

        for node in write_nodes:
            node.send(sync.Method.Create, {'foo': 'bar'})

        reads = []
        while True:
            result = n4.fetch()
            if result is None:
                break
            reads.append(result)
        assert len(reads) == len(write_nodes)
        assert n1.fetch() is None
        assert n2.fetch() is None
        assert n3.fetch() is None

    def test_sync_multi_write_multi_read(self):
        network = sync.Network.get()
        network.fetch_before_send = False
        network.save()

        n1 = sync.Node.create(create=True, read=True)
        n2 = sync.Node.create(create=True, read=True)
        n3 = sync.Node.create(create=True, read=True)
        n4 = sync.Node.create(create=True, read=True)

        nodes = [n1, n2, n3, n4]

        for node in nodes:
            node.send(sync.Method.Create, {'foo': 'bar'})

        for node in nodes:
            reads = []
            while True:
                result = node.fetch()
                if result is None:
                    break
                reads.append(result)
            assert len(reads) == len(nodes)-1

    def test_schema_sync_two_nodes(self, test_schema):
        network = sync.Network.get()
        network.fetch_before_send = False
        network.save()

        n1 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        n2 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)

        n1_records = []
        n2_records = []

        # Send the first half of the test data from one node, then the
        # other remainder from the second node.
        for i in range(len(self.data)):
            item = self.data[i]

            if i < round(len(self.data) / 2):
                n1.send(sync.Method.Create, item)
                n1_records.append(item)
            else:
                n2.send(sync.Method.Create, item)
                n2_records.append(item)

        # Fetch available data for each node.
        while True:
            result = n1.fetch()
            if result is None:
                break
            n1.acknowledge(result.id)
            n1_records.append(result.payload)
        while True:
            result = n2.fetch()
            if result is None:
                break
            n2.acknowledge(result.id)
            n2_records.append(result.payload)

        # Verify the nodes are synced by checking they all have the
        # same data as in self.data.
        assert len(self.data) == len(n1_records)
        assert len(self.data) == len(n2_records)

        self.data, n1_records = [sorted(l, key=itemgetter('id'))
                                 for l in (self.data, n1_records)]
        pairs = zip(self.data, n1_records)
        assert not any(x != y for x, y in pairs)

        self.data, n2_records = [sorted(l, key=itemgetter('id'))
                                 for l in (self.data, n2_records)]
        pairs = zip(self.data, n2_records)
        assert not any(x != y for x, y in pairs)

    def test_sync_duplicate_remote_id(self):
        n1 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        n2 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)

        n1.send(sync.Method.Create, {'foo': 'bar'}, remote_id='abc')
        message = n2.fetch()
        n2.acknowledge(message.id, "foo")

        n1.send(sync.Method.Create, {'foo': 'bar'}, remote_id='def')
        message = n2.fetch()
        with pytest.raises(exceptions.InvalidOperationError):
            n2.acknowledge(message.id, "foo")

        n2.send(sync.Method.Create, {'foo': 'bar'}, remote_id='123')

        with pytest.raises(exceptions.InvalidOperationError):
            n2.send(sync.Method.Create, {'foo': 'bar'},
                    remote_id='123')

    def test_node__get_message(self):
        node = sync.Node.create(create=True, read=True, update=True,
                                delete=True)
        with pytest.raises(exceptions.NotFoundError):
            node._get_message(sync.generate_id())

    def test_node_send_errors(self):
        node = sync.Node.create(create=True, read=True, update=True,
                                delete=True)
        with pytest.raises(exceptions.InvalidOperationError):
            node.send(sync.Method.Read)
        with pytest.raises(exceptions.InvalidOperationError):
            node.send(sync.Method.Create, record_id='not none')

    def test_message_get_history_empty(self):
        message = sync.Message()
        assert [] == message.changes()

    def test_message_send_raise_errors(self):
        node = sync.Node.create(create=True, read=True, update=True,
                                delete=True)
        original = sync.Message._execute
        sync.Message._execute = error_fun
        with pytest.raises(MockError):
            node.send(sync.Method.Create, {})
        sync.Message._execute = original

    def test_message_fetch_raise_errors(self):
        n1 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        n2 = sync.Node.create(create=True, read=True, update=True,
                              delete=True)
        n1.send(sync.Method.Create, {'foo': 'bar'}, remote_id='abc')
        original = sync.Message.update
        sync.Message.update = error_fun
        with pytest.raises(MockError):
            n2.fetch()
        sync.Message.update = original

    def test_storage_get_many_empty(self):
        nodes = sync.Node.get()
        assert nodes == []


class TestBaseStorage():

    def test_base_storage(self):
        storage = Storage()

        with pytest.raises(NotImplementedError):
            storage.connect()
        with pytest.raises(NotImplementedError):
            storage.disconnect()
        with pytest.raises(NotImplementedError):
            storage.drop()
        with pytest.raises(NotImplementedError):
            storage.start_transaction()
        with pytest.raises(NotImplementedError):
            storage.commit()
        with pytest.raises(NotImplementedError):
            storage.rollback()
        with pytest.raises(NotImplementedError):
            storage.save_network(None)
        with pytest.raises(NotImplementedError):
            storage.save_node(None)
        with pytest.raises(NotImplementedError):
            storage.save_message(None)
        with pytest.raises(NotImplementedError):
            storage.save_change(None)
        with pytest.raises(NotImplementedError):
            storage.save_record(None)
        with pytest.raises(NotImplementedError):
            storage.save_remote(None)
        with pytest.raises(NotImplementedError):
            storage.get_network()
        with pytest.raises(NotImplementedError):
            storage.get_node(None)
        with pytest.raises(NotImplementedError):
            storage.get_record(None)
        with pytest.raises(NotImplementedError):
            storage.get_remote(None)
        with pytest.raises(NotImplementedError):
            storage.get_message()
        with pytest.raises(NotImplementedError):
            storage.get_message_count()
        with pytest.raises(NotImplementedError):
            storage.get_nodes()
        with pytest.raises(NotImplementedError):
            storage.get_records()
        with pytest.raises(NotImplementedError):
            storage.get_changes(None)
        with pytest.raises(NotImplementedError):
            storage.update_messages(None, None, None)
