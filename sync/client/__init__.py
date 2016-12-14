import json
import requests
import six

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin


class ClientError(Exception):

    def __init__(self, response):
        self.response == response


class ClientObject(object):
    pass


class Client(object):

    def __init__(self, base_url):
        self.base_url = base_url

    def _check_response(self, response):
        if response.status_code in (200, 201):
            return

        raise ClientError(response)

    def _parse_response(self, response):
        data = json.loads(response.text)
        obj = ClientObject()
        for key, value in six.iteritems(data):
            setattr(obj, key, value)
        return obj

    def create_system(self, name, schema, fetch_before_send=False):
        url = urljoin(self.base_url, '/systems')
        data = {
            'name': name,
            'schema': schema,
            'fetch_before_send': fetch_before_send
        }
        data = json.dumps(data)
        response = requests.post(url, data=data)
        self._check_response(response)
        return self._parse_response(response)
