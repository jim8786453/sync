import falcon
import json
import mongomock
import pytest

from falcon.testing.client import TestClient as tc

import sync

from sync import exceptions
from sync.conftest import postgresql
from sync.constants import Backend
from sync.http import errors, server, utils


@pytest.mark.parametrize('storage_class', Backend.All)
class TestHttp():

    @pytest.fixture(autouse=True)
    def storage(self, request, session_setup, storage_class):
        sync.settings.STORAGE_CLASS = storage_class
        if storage_class == Backend.Postgres:
            sync.settings.POSTGRES_CONNECTION = postgresql.url()
        elif storage_class == Backend.Mongo:
            sync.storage.test_mongo_client = mongomock.MongoClient()

        yield

        if sync.s is not None:
            sync.s.drop()
            sync.s.disconnect()

    @pytest.fixture(autouse=True)
    def client(self):
        self.client = tc(server.api)

    def setup_system(self):
        body = {
            'name': 'test',
            'fetch_before_send': True,
            'schema': {
                'title': 'Example Schema',
                'type': 'object',
                'properties': {
                    'firstName': {
                        'type': 'string'
                    },
                    'lastName': {
                        'type': 'string'
                    },
                    'age': {
                        'description': 'Age in years',
                        'type': 'integer',
                        'minimum': 0
                    }
                },
                'required': ['firstName', 'lastName']
            }
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/systems', body=body_json)
        self.system_id = str(result.json['id'])

    def setup_nodes(self):
        body = {
            'name': 'node 1',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        url = '/systems/{0}/nodes'.format(self.system_id)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 201
        self.node_1 = result.json
        self.node_1_headers = {
            'X-Sync-System-Id': self.system_id,
            'X-Sync-Node-Id': str(self.node_1['id'])
        }

        body = {
            'name': 'node 2',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 201
        self.node_2 = result.json
        self.node_2_headers = {
            'X-Sync-System-Id': self.system_id,
            'X-Sync-Node-Id': str(self.node_2['id'])
        }

    def test_http_utils_inflate(self):
        with pytest.raises(exceptions.InvalidJsonError):
            utils.inflate("{", None, None)

    def test_http_utils_obj_or_404(self):
        with pytest.raises(falcon.HTTPNotFound):
            utils.obj_or_404(None)

    def test_http_systems(self, request):
        # POST 400
        body = {}
        body_json = json.dumps(body)
        url = '/systems'
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 400

        # POST 200
        body = {
            'name': 'test',
            'fetch_before_send': True,
            'schema': {}
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 201

        # system_id is required for GET methods
        system_id = str(result.json['id'])

        # GET 404
        url = '/systems/foo'
        result = self.client.simulate_get(url)
        assert result.status_code == 404

        # GET 200
        url = '/systems/{0}'.format(system_id)
        result = self.client.simulate_get(url)
        assert result.status_code == 200

        # PATCH 200
        body = {
            'name': 'new_value'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_patch(url, body=body_json)
        assert result.status_code == 200

    def test_http_nodes(self, request):
        self.setup_system()

        # POST 400 mising name
        body = {
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        url = '/systems/{0}/nodes'.format(self.system_id)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 400

        # POST 200
        body = {
            'name': 'node 1',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        url = '/systems/{0}/nodes'.format(self.system_id)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 201
        node_1_id = result.json['id']
        assert node_1_id is not None

        # POST 200 for a second node
        body = {
            'name': 'node 2',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        url = '/systems/{0}/nodes'.format(self.system_id)
        result = self.client.simulate_post(url, body=body_json)
        assert result.status_code == 201
        node_2_id = result.json['id']
        assert node_2_id is not None

        # GET 200
        url = '/systems/{0}/nodes/'.format(self.system_id)
        result = self.client.simulate_get(url)
        assert result.status_code == 200

        # GET 200
        url = '/systems/{0}/nodes/{1}'.format(self.system_id, node_1_id)
        result = self.client.simulate_get(url)
        assert result.status_code == 200

        # GET 404
        url = '/systems/{0}/nodes/foo'.format(self.system_id)
        result = self.client.simulate_get(url)
        assert result.status_code == 404

    def test_http_message_list(self, request):
        self.setup_system()
        self.setup_nodes()

        # POST 200
        url = '/messages'
        body = {
            'method': 'create',
            'payload': {
                'firstName': 'test',
                'lastName': 'test'
            },
            'remote_id': "1"
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # POST 200
        url = '/messages'
        body['remote_id'] = '2'
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

    def test_http_message_headers(self, request):
        self.setup_system()
        self.setup_nodes()

        url = '/messages/pending'
        headers = {
            'X-Sync-System-Id': self.system_id
        }
        result = self.client.simulate_get(url, headers=headers)
        assert result.status_code == 400

        headers = {
            'X-Sync-Node-Id': str(self.node_1['id'])
        }
        result = self.client.simulate_get(url, headers=headers)
        assert result.status_code == 400

        headers = {
            'X-Sync-System-Id': 'foo',
            'X-Sync-Node-Id': str(self.node_1['id'])
        }
        result = self.client.simulate_get(url, headers=headers)
        assert result.status_code == 404

        headers = {
            'X-Sync-System-Id': self.system_id,
            'X-Sync-Node-Id': 'foo'
        }
        result = self.client.simulate_get(url, headers=headers)
        assert result.status_code == 404

    def test_http_message_pending(self, request):
        self.setup_system()
        self.setup_nodes()

        # Node 2. Check if it has pending messages.
        url = '/messages/pending'
        result = self.client.simulate_get(url, headers=self.node_2_headers)
        assert result.json is False

        # Node 1. Send a message.
        url = '/messages'
        body = {
            'method': 'create',
            'payload': {
                'firstName': 'test',
                'lastName': 'test'
            },
            'remote_id': '0001'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # Node 2. Check if it has pending messages.
        url = '/messages/pending'
        result = self.client.simulate_get(url, headers=self.node_2_headers)
        assert result.json is True

    def test_http_message_send_and_ack(self, request):
        self.setup_system()
        self.setup_nodes()

        # POST 200
        url = '/messages'
        body = {
            'method': 'create',
            'payload': {
                'firstName': 'test',
                'lastName': 'test'
            },
            'remote_id': "1"
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # POST 200
        url = '/messages'
        body['remote_id'] = '2'
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # POST 200
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 200
        message_1_id = result.json['id']

        # POST 200
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 200
        message_2_id = result.json['id']

        # POST 204
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 204

        # POST 200
        url = '/messages/{0}'.format(message_1_id)
        body = {
            'success': True,
            'remote_id': "1"
        }
        body_json = json.dumps(body)
        result = self.client.simulate_patch(url, body=body_json,
                                            headers=self.node_2_headers)
        assert result.status_code == 200

        # POST 200
        url = '/messages/{0}'.format(message_2_id)
        body = {
            'success': False,
            'reason': 'This is a reason.'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_patch(url, body=body_json,
                                            headers=self.node_2_headers)
        assert result.status_code == 200

    def test_http_send_with_remote_ids(self, request):
        self.setup_system()
        self.setup_nodes()

        # Node 1: Create a record and attach a remote id.
        url = '/messages'
        body = {
            'method': 'create',
            'payload': {
                'firstName': 'test',
                'lastName': 'test'
            },
            'remote_id': '0001'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # Node 1: Update the record using the remote id.
        body['method'] = 'update'
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_1_headers)
        assert result.status_code == 200

        # Node 1: Ensure the node has no pending messages.
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_1_headers)
        assert result.status_code == 204

        # Node 2: Fetch message.
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 200

        # Node 2. Acknowledge with remote_id
        url = '/messages/' + result.json['id']
        body = {
            'success': True,
            'remote_id': 'abcd'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_patch(url, body=body_json,
                                            headers=self.node_2_headers)
        assert result.status_code == 200

        # Node 2: Fetch next message and check remote_id.
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 200
        assert result.json['remote_id'] == 'abcd'

        # Node 2. Update using the remote_id.
        url = '/messages'
        body = {
            'method': 'update',
            'payload': {
                'firstName': 'changed',
                'lastName': 'changed'
            },
            'remote_id': 'abcd'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.node_2_headers)
        assert result.status_code == 200

        # Node 2. Sync the record again.
        url = '/systems/{0}/nodes/{1}/sync'.format(self.system_id,
                                                   str(self.node_2['id']))
        result = self.client.simulate_post(url)
        assert result.status_code == 200

        # Node 2. Fetch the record and check the remote_id.
        url = '/messages/next'
        result = self.client.simulate_post(url, headers=self.node_2_headers)
        assert result.status_code == 200
        assert result.json['remote_id'] == 'abcd'


def test_utils_json_serial():
    node = sync.Node()
    node.id = 'foo'
    serial = utils.json_serial(node)
    assert serial['id'] == node.id

    value = {}.keys()
    serial = utils.json_serial(value)
    assert serial == []

    value = {}.values()
    serial = utils.json_serial(value)
    assert serial == []

    # Type not handled.
    value = 10
    with pytest.raises(TypeError):
        utils.json_serial(value)


def test_raise_http_invalid_request_error():
    with pytest.raises(falcon.HTTPBadRequest):
        errors.raise_http_invalid_request(None, None, None, None)
