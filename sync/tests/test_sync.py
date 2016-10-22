import json
import jsonschema
import os
import os.path
import pytest


from operator import itemgetter

import sync

from sync import storage
from conftest import postgresql


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
    mock_storage.connect(sync.generate_id())
    return mock_storage


def generate_postgresql_storage():
    """Return a sync.storage.PostgresStorage object.

    """
    postgres_storage = storage.PostgresStorage(sync.generate_id())
    postgres_storage.connect(postgresql.url())
    return postgres_storage


storage_generators = [
    generate_mock_storage,
    generate_postgresql_storage
]


@pytest.mark.parametrize('storage_fun', storage_generators)
class TestSync():

    @pytest.fixture(autouse=True)
    def storage(self, request, session_setup, storage_fun):
        self.storage = storage_fun()

        sync.init(self.storage)
        sync.Settings.init('test', {}, True)

        assert sync.s == self.storage

        yield

        sync.s.drop()

    @pytest.fixture(autouse=False)
    def test_schema(self, request):
        settings = sync.Settings.get()
        settings.schema = test_schema()
        settings.save()
        self.data = test_data()

    def test_settings(self):
        settings = sync.Settings.get()
        settings.name = 'Mock'
        settings.schema = {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string"
                }
            }
        }
        settings.save()
        returned = sync.Settings.get()
        assert settings.id is not None
        assert settings == returned

        settings.name = 'Update'
        settings.save()
        assert settings != returned
        returned = sync.Settings.get()
        assert settings == returned

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

    def test_error(self):
        message = sync.Message()
        message.method = sync.Method.Create
        message.save()

        error = sync.Error()
        error.message_id = message.id
        error.text = "Message"
        error.save()
        returned = message.errors()

        assert len(returned) == 1
        assert returned[0] == error

        with pytest.raises(Exception):
            error.save()

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
        assert result == sync.merge_patch(original, patch)

        original = {"a": "b"}
        patch = {"b": "c"}
        result = {"a": "b", "b": "c"}
        assert result == sync.merge_patch(original, patch)

        original = {"a": "b"}
        patch = {"a": None}
        result = {}
        assert result == sync.merge_patch(original, patch)

        original = {"a": "b", "b": "c"}
        patch = {"a": None}
        result = {"b": "c"}
        assert result == sync.merge_patch(original, patch)

        original = {"a": ["b"]}
        patch = {"a": "c"}
        result = {"a": "c"}
        assert result == sync.merge_patch(original, patch)

        original = {"a": "c"}
        patch = {"a": ["b"]}
        result = {"a": ["b"]}
        assert result == sync.merge_patch(original, patch)

        original = {"a": [{"b": "c"}]}
        patch = {"a": [1]}
        result = {"a": [1]}
        assert result == sync.merge_patch(original, patch)

        original = ["a", "b"]
        patch = ["c", "d"]
        result = ["c", "d"]
        assert result == sync.merge_patch(original, patch)

        original = {"a": "b"}
        patch = ["c"]
        result = ["c"]
        assert result == sync.merge_patch(original, patch)

        original = {"a": "foo"}
        patch = None
        result = None
        assert result == sync.merge_patch(original, patch)

        original = {"a": "foo"}
        patch = "bar"
        result = "bar"
        assert result == sync.merge_patch(original, patch)

        original = {"e": None}
        patch = {"a": 1}
        result = {"e": None, "a": 1}
        assert result == sync.merge_patch(original, patch)

        original = [1, 2]
        patch = {"a": "b", "c": None}
        result = {"a": "b"}
        assert result == sync.merge_patch(original, patch)

        original = {}
        patch = {"a": {"bb": {"ccc": None}}}
        result = {"a": {"bb": {}}}
        assert result == sync.merge_patch(original, patch)

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
        settings = sync.Settings.get()
        settings.fetch_before_send = True
        message = sync.Message()
        message.method = sync.Method.Create
        message.payload = {}
        message.origin_id = node_2.id
        message._origin = node_2
        message._settings = settings
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
        message._settings = settings
        with pytest.raises(sync.exceptions.InvalidOperationError):
            message._validate()

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

        with pytest.raises(sync.exceptions.InvalidOperationError) as e:
            message.update(sync.State.Pending)
            assert e.message == sync.Text.MessageStateInvalid.format(
                sync.State.Acknowledged, sync.State.Pending)

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
        assert len(message.errors()) == 1
        assert message.errors()[0].text == "reason"

    def test_message_send(self):
        node_1 = sync.Node.create()

        with pytest.raises(sync.exceptions.InvalidOperationError):
            message = sync.Message.send(node_1.id, sync.Method.Create,
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

        settings = sync.Settings.get()
        settings.schema = {'type': 'string'}
        settings.save()

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
        settings = sync.Settings.get()
        settings.fetch_before_send = False
        settings.save()

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
        settings = sync.Settings.get()
        settings.fetch_before_send = False
        settings.save()

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
