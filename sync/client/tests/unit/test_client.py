import httpretty
import json
import pytest

from sync.client import Client, ClientError


class TestClient():

    @pytest.fixture(autouse=True)
    def client(self):
        self.client = Client('http://sync.test/')

    @httpretty.activate
    def test_client_create_network(self):
        mock_response = {
            'name': 'Test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
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
            'name': 'Test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        url = 'http://sync.test/networks/' + mock_response['id']
        httpretty.register_uri(httpretty.GET, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        network = self.client.get_network(mock_response['id'])
        assert network.id == mock_response['id']
        assert network.name == mock_response['name']
