import pytest
import json

from falcon.testing.client import TestClient as tc

import sync

from sync.conftest import postgresql
from sync.http import server


class TestHttp():

    @pytest.fixture(autouse=True)
    def storage(self, request, session_setup):
        sync.settings.POSTGRES_CONNECTION = postgresql.url()

        yield

        sync.s.drop()
        sync.s.disconnect()

    @pytest.fixture(autouse=True)
    def client(self):
        self.client = tc(server.api)

    def setup_headers(self):
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
        result = self.client.simulate_post('/', body=body_json)
        self.headers = {'X-Sync-Id': str(result.json['id'])}

    def setup_nodes(self):
        body = {
            'name': 'node 1',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/node', body=body_json,
                                           headers=self.headers)
        assert result.status_code == 201
        self.node_1 = result.json

        body = {
            'name': 'node 2',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/node', body=body_json,
                                           headers=self.headers)
        assert result.status_code == 201
        self.node_2 = result.json

    def test_http_system(self, request):
        # POST 400 /
        body = {}
        body_json = json.dumps(body)
        result = self.client.simulate_post('/', body=body_json)
        assert result.status_code == 400

        # POST 200 /
        body = {
            'name': 'test',
            'fetch_before_send': True,
            'schema': {}
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/', body=body_json)
        assert result.status_code == 201

        # system_id is required for GET methods
        system_id = str(result.json['id'])

        # GET 400 /
        result = self.client.simulate_get('/')
        assert result.json['title'] == 'Missing header value'
        assert result.status_code == 400

        # GET 404 /
        headers = {
            'X-Sync-Id': 'foo'
        }
        result = self.client.simulate_get('/', headers=headers)
        assert result.status_code == 404

        # GET 200 /
        headers = {
            'X-Sync-Id': str(system_id)
        }
        result = self.client.simulate_get('/', headers=headers)
        assert result.status_code == 200

        # PATCH 200 /
        body = {
            'name': 'new_value'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_patch('/', body=body_json,
                                            headers=headers)
        assert result.status_code == 200

    def test_http_node(self, request):
        self.setup_headers()

        # POST 400 /node
        body = {
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/node', body=body_json,
                                           headers=self.headers)
        assert result.status_code == 400

        # POST 200 /node
        body = {
            'name': 'node 1',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/node', body=body_json,
                                           headers=self.headers)
        assert result.status_code == 201
        node_1_id = result.json['id']
        assert node_1_id is not None

        # POST 200 /node for a second node
        body = {
            'name': 'node 2',
            'create': True,
            'read': True,
            'update': True,
            'delete': True
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post('/node', body=body_json,
                                           headers=self.headers)
        assert result.status_code == 201
        node_2_id = result.json['id']
        assert node_2_id is not None

        # GET 200 /node/{id}
        url = '/node/' + node_1_id
        result = self.client.simulate_get(url, headers=self.headers)
        assert result.status_code == 200

        # GET 404 /node/{id}
        url = '/node/foo'
        result = self.client.simulate_get(url, headers=self.headers)
        assert result.status_code == 404

        # POST 200 /node/{id}/send
        url = '/node/{0}/send'.format(node_1_id)
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
                                           headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/send
        url = '/node/{0}/send'.format(node_1_id)
        body['remote_id'] = '2'
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200
        message_1_id = result.json['id']

        # POST 200 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200
        message_2_id = result.json['id']

        # POST 204 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 204

        # POST 404 /node/{id}/fetch
        url = '/node/foo/fetch'
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 404

        # POST 200 /node/{id}/ack
        url = '/node/{0}/ack'.format(node_2_id)
        body = {
            'message_id': message_1_id,
            'remote_id': "1"
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/fail
        url = '/node/{0}/fail'.format(node_2_id)
        body = {
            'message_id': message_2_id,
            'reason': 'This is a reason.'
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/sync
        url = '/node/{0}/sync'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200

        # POST 200 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200

        # POST 204 /node/{id}/fetch
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 204

    def test_http_node_with_remote_ids(self, request):
        self.setup_headers()
        self.setup_nodes()

        # Node 1: Create a record and attach a remote id.
        node_1_id = self.node_1['id']
        url = '/node/{0}/send'.format(node_1_id)
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
                                           headers=self.headers)
        assert result.status_code == 200

        # Node 1: Update the record using the remote id.
        body['method'] = 'update'
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.headers)
        assert result.status_code == 200

        # Node 1: Ensure the node has no pending messages.
        url = '/node/{0}/fetch'.format(node_1_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 204

        # Node 2: Fetch message.
        node_2_id = self.node_2['id']
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200

        # Node 2. Acknowledge with remote_id
        url = '/node/{0}/ack'.format(node_2_id)
        body = {
            'message_id': result.json['id'],
            'remote_id': "abcd"
        }
        body_json = json.dumps(body)
        result = self.client.simulate_post(url, body=body_json,
                                           headers=self.headers)
        assert result.status_code == 200

        # Node 2: Fetch message and check remote_id.
        node_2_id = self.node_2['id']
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200
        assert result.json['remote_id'] == 'abcd'

        # Node 2. Update using the remote_id.
        url = '/node/{0}/send'.format(node_2_id)
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
                                           headers=self.headers)
        assert result.status_code == 200

        # Node 2. Sync the record again.
        url = '/node/{0}/sync'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200

        # Node 2. Fetch the record and check the remote_id.
        url = '/node/{0}/fetch'.format(node_2_id)
        result = self.client.simulate_post(url, headers=self.headers)
        assert result.status_code == 200
        assert result.json['remote_id'] == 'abcd'

    def test_http_inflate_invalid_json(self, request):
        body_json = "{,"
        result = self.client.simulate_post('/', body=body_json)
        assert result.status_code == 400
