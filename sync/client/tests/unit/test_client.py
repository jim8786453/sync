import httpretty
import json
import pytest

from sync.client import Client


class TestClient():

    @pytest.fixture(autouse=True)
    def client(self):
        self.client = Client('http://sync.test/')

    @httpretty.activate
    def test_client_create_system(self):
        mock_response = {
            'name': 'Test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        url = 'http://sync.test/systems'
        httpretty.register_uri(httpretty.POST, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=201)
        system = self.client.create_system('test', {}, True)
        assert system.id == mock_response['id']
        assert system.name == mock_response['name']

    @httpretty.activate
    def test_client_get_system(self):
        mock_response = {
            'name': 'Test',
            'nodes': [],
            'id': '32ca9377-5ef6-400b-b39b-d9fcdaa51d0d',
            'fetch_before_send': True,
            'schema': {}
        }
        url = 'http://sync.test/systems/' + mock_response['id']
        httpretty.register_uri(httpretty.GET, url,
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=200)
        system = self.client.get_system(mock_response['id'])
        assert system.id == mock_response['id']
        assert system.name == mock_response['name']
