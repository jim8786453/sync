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
        httpretty.register_uri(httpretty.POST, 'http://sync.test/systems',
                               body=json.dumps(mock_response),
                               content_type='application/json',
                               status=201)
        system = self.client.create_system('test', {}, True)
        assert system.id == mock_response['id']
        assert system.name == mock_response['name']
