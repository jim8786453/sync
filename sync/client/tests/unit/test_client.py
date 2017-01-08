import httpretty
import json
import jsonschema
import pytest

from sync import schema
from sync.client import Client, ClientError


class TestClient():

    @pytest.fixture(autouse=True)
    def client(self):
        self.client = Client('http://sync.test/')

    @httpretty.activate
    def test_client_create_network(self):
        mock_response = {
            'name': 'test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        jsonschema.validators.Draft4Validator(
            schema.network_get).validate(mock_response)
        url = 'http://sync.test/networks'
        httpretty.register_uri(httpretty.POST, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=201)
        network = self.client.create_network('test', {}, True)
        assert network.id == mock_response['id']
        assert network.name == mock_response['name']

    @httpretty.activate
    def test_client_create_network_error(self):
        url = 'http://sync.test/networks'
        httpretty.register_uri(httpretty.POST, url,
                               body=json.dumps({}),
                               content_type='application/json',
                               status=400)
        with pytest.raises(ClientError):
            self.client.create_network(None, None, None)

    @httpretty.activate
    def test_client_get_network(self):
        mock_response = {
            'name': 'test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        jsonschema.validators.Draft4Validator(
            schema.network_get).validate(mock_response)
        network_id = mock_response['id']
        url = 'http://sync.test/networks/' + network_id
        httpretty.register_uri(httpretty.GET, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        network = self.client.get_network(network_id)
        assert network.id == mock_response['id']
        assert network.name == mock_response['name']

    @httpretty.activate
    def test_client_update_network(self):
        mock_response = {
            'name': 'test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        jsonschema.validators.Draft4Validator(
            schema.network_get).validate(mock_response)
        network_id = mock_response['id']
        url = 'http://sync.test/networks/' + network_id
        httpretty.register_uri(httpretty.PATCH, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        network = self.client.update_network(network_id,
                                             'test', {}, True)
        assert network.id == mock_response['id']
        assert network.name == mock_response['name']

    @httpretty.activate
    def test_client_create_node(self):
        mock_response = {
            'name': 'test',
            'read': True,
            'create': True,
            'update': True,
            'id': '619120e2-b8e5-40a9-9fab-93a9524dc8c0',
            'delete': True
        }
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(mock_response)
        network_id = '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d'
        url = 'http://sync.test/networks/{0}/nodes'.format(network_id)
        httpretty.register_uri(httpretty.POST, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=201)
        node = self.client.create_node(network_id, 'test')
        assert node.id == mock_response['id']
        assert node.name == mock_response['name']

    @httpretty.activate
    def test_client_update_node(self):
        mock_response = {
            'name': 'test',
            'read': True,
            'create': True,
            'update': True,
            'id': '619120e2-b8e5-40a9-9fab-93a9524dc8c0',
            'delete': True
        }
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(mock_response)
        network_id = '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d'
        node_id = mock_response['id']
        url_template = 'http://sync.test/networks/{0}/nodes/{1}'
        url = url_template.format(network_id, node_id)
        httpretty.register_uri(httpretty.PATCH, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        node = self.client.update_node(network_id, node_id, name='test')
        assert node.id == mock_response['id']
        assert node.name == mock_response['name']

    @httpretty.activate
    def test_client_get_nodes(self):
        mock_response = [
            {
                'name': 'test',
                'read': True,
                'create': True,
                'update': True,
                'id': '619120e2-b8e5-40a9-9fab-93a9524dc8c0',
                'delete': True
            },
            {
                'name': 'test2',
                'read': True,
                'create': True,
                'update': True,
                'id': '719120e2-b8e5-40a9-9fab-93a9524dc8c0',
                'delete': True
            }
        ]
        jsonschema.validators.Draft4Validator(
            schema.nodes_get).validate(mock_response)
        network_id = '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d'
        url_template = 'http://sync.test/networks/{0}/nodes'
        url = url_template.format(network_id)
        httpretty.register_uri(httpretty.GET, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        nodes = self.client.get_nodes(network_id)
        assert len(nodes) == 2

    @httpretty.activate
    def test_client_get_node(self):
        mock_response = {
            'name': 'test',
            'read': True,
            'create': True,
            'update': True,
            'id': '619120e2-b8e5-40a9-9fab-93a9524dc8c0',
            'delete': True
        }
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(mock_response)
        network_id = '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d'
        node_id = mock_response['id']
        url_template = 'http://sync.test/networks/{0}/nodes/{1}'
        url = url_template.format(network_id, node_id)
        httpretty.register_uri(httpretty.GET, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        node = self.client.get_node(network_id, node_id)
        assert node.id == mock_response['id']
        assert node.name == mock_response['name']

    @httpretty.activate
    def test_client_sync_node(self):
        mock_response = None
        network_id = '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d'
        node_id = '619120e2-b8e5-40a9-9fab-93a9524dc8c0'
        url_template = 'http://sync.test/networks/{0}/nodes/{1}/sync'
        url = url_template.format(network_id, node_id)
        httpretty.register_uri(httpretty.POST, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        self.client.sync_node(network_id, node_id)
        assert True
