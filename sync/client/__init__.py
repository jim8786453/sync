import json
import requests
import six

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin


class ClientError(Exception):

    def __init__(self, message, response):
        super(Exception, self).__init__(message)
        self.response = response


class ClientObject(object):
    pass


class Client(object):

    def __init__(self, base_url):
        self.base_url = base_url

    def _check_response(self, response):
        if response.status_code in (200, 201):
            return

        raise ClientError('Error %s' % response.status_code, response)

    def _inflate_object(self, data, class_):
        obj = class_()
        for key, value in six.iteritems(data):
            setattr(obj, key, value)
        return obj

    def _parse_response(self, response, class_):
        self._check_response(response)
        data = json.loads(response.text)
        if data is None:
            return None
        elif isinstance(data, list):
            result = []
            for item in data:
                obj = self._inflate_object(item, class_)
                result.append(obj)
            return result
        else:
            obj = self._inflate_object(data, class_)
            return obj

    def create_network(self, name, schema, fetch_before_send=False):
        path = '/networks'
        url = urljoin(self.base_url, path)
        data = {
            'name': name,
            'schema': schema,
            'fetch_before_send': fetch_before_send
        }
        data = json.dumps(data)
        response = requests.post(url, data=data)
        class_ = type('Network', (ClientObject,), {})
        return self._parse_response(response, class_)

    def get_network(self, network_id):
        path = '/networks/' + network_id
        url = urljoin(self.base_url, path)
        response = requests.get(url)
        class_ = type('Network', (ClientObject,), {})
        return self._parse_response(response, class_)

    def update_network(self, network_id, name=None, schema=None,
                       fetch_before_send=None):
        path = '/networks/' + network_id
        url = urljoin(self.base_url, path)
        data = {}
        if name is not None:
            data['name'] = name
        if schema is not None:
            data['schema'] = schema
        if fetch_before_send is not None:
            data['fetch_before_send'] = fetch_before_send
        response = requests.patch(url, data=data)
        class_ = type('Network', (ClientObject,), {})
        return self._parse_response(response, class_)

    def create_node(self, network_id, name, create=True, read=True,
                    update=True, delete=True):
        path = '/networks/{0}/nodes'.format(network_id)
        url = urljoin(self.base_url, path)
        data = {
            'name': name,
            'create': create,
            'read': read,
            'update': update,
            'delete': delete
        }
        data = json.dumps(data)
        response = requests.post(url, data=data)
        class_ = type('Node', (ClientObject,), {})
        return self._parse_response(response, class_)

    def update_node(self, network_id, node_id, name=None,
                    create=None, read=None, update=None, delete=None):
        path = '/networks/{0}/nodes/{1}'.format(network_id, node_id)
        url = urljoin(self.base_url, path)
        data = {}
        if name is not None:
            data['name'] = name
        if create is not None:
            data['create'] = create
        if read is not None:
            data['read'] = read
        if update is not None:
            data['update'] = update
        if delete is not None:
            data['delete'] = delete
        data = json.dumps(data)
        response = requests.patch(url, data=data)
        class_ = type('Node', (ClientObject,), {})
        return self._parse_response(response, class_)

    def get_nodes(self, network_id):
        path = '/networks/{0}/nodes'.format(network_id)
        url = urljoin(self.base_url, path)
        response = requests.get(url)
        class_ = type('Node', (ClientObject,), {})
        return self._parse_response(response, class_)

    def get_node(self, network_id, node_id):
        path = '/networks/{0}/nodes/{1}'.format(network_id, node_id)
        url = urljoin(self.base_url, path)
        response = requests.get(url)
        class_ = type('Node', (ClientObject,), {})
        return self._parse_response(response, class_)

    def sync_node(self, network_id, node_id):
        path = '/networks/{0}/nodes/{1}/sync'.format(network_id, node_id)
        url = urljoin(self.base_url, path)
        response = requests.post(url)
        assert response.status_code == 200
