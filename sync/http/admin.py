import json
import jsonschema
import falcon

import sync

from sync import schema
from sync.http import utils
from sync.storage import init_storage


def init(network_id):
    init_storage(network_id, create_db=False)


class NetworkList:

    def on_post(self, req, resp):
        network_id = sync.generate_id()
        init_storage(network_id, create_db=True)
        json_data = req.stream.read()
        network = utils.inflate(json_data, sync.Network(),
                                schema.network_create)
        network.save()
        network = network.as_dict(with_id=True)
        resp.body = json.dumps(network, default=utils.json_serial)
        resp.status = falcon.HTTP_201


class Network:

    def on_get(self, req, resp, network_id):
        init(network_id)
        network = sync.Network.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.network_get).validate(network)
        resp.body = json.dumps(network, default=utils.json_serial)

    def on_patch(self, req, resp, network_id):
        init(network_id)
        network = sync.Network.get()
        json_data = req.stream.read()
        network = utils.inflate(json_data, network, schema.network_update)
        network.save()
        network = sync.Network.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.network_get).validate(network)
        resp.body = json.dumps(network, default=utils.json_serial)


class NodeList:

    def on_get(self, req, resp, network_id):
        init(network_id)
        nodes = sync.Node.get()
        result = [n.as_dict(with_id=True) for n in nodes]
        jsonschema.validators.Draft4Validator(
            schema.nodes_get).validate(result)
        resp.body = utils.json.dumps(nodes, default=utils.json_serial)

    def on_post(self, req, resp, network_id):
        init(network_id)
        json_data = req.stream.read()
        node = utils.inflate(json_data, sync.Node(), schema.node_create)
        node.save()
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = utils.json.dumps(node, default=utils.json_serial)
        resp.status = falcon.HTTP_201


class Node:

    def on_get(self, req, resp, network_id, node_id):
        init(network_id)
        node = sync.Node.get(node_id)
        utils.obj_or_404(node)
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = json.dumps(node, default=utils.json_serial)


class NodeSync:

    def on_post(self, req, resp, network_id, node_id):
        init(network_id)
        node = sync.Node.get(node_id)
        utils.obj_or_404(node)
        node.sync()
