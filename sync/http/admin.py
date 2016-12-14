import json
import jsonschema
import falcon

import sync

from sync.http import schema, utils
from sync.storage import init_storage


def init(system_id):
    init_storage(system_id, create_db=False)


class SystemList:

    def on_post(self, req, resp):
        system_id = sync.generate_id()
        init_storage(system_id, create_db=True)
        json_data = req.stream.read()
        system = utils.inflate(json_data, sync.System(), schema.system_post)
        system.save()
        system = system.as_dict(with_id=True)
        nodes = sync.Node.get()
        system['nodes'] = nodes
        resp.body = json.dumps(system, default=utils.json_serial)
        resp.status = falcon.HTTP_201


class System:

    def on_get(self, req, resp, system_id):
        init(system_id)
        system = sync.System.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.system_get).validate(system)
        resp.body = json.dumps(system, default=utils.json_serial)

    def on_patch(self, req, resp, system_id):
        init(system_id)
        system = sync.System.get()
        json_data = req.stream.read()
        system = utils.inflate(json_data, system, schema.system_patch)
        system.save()
        system = sync.System.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.system_get).validate(system)
        resp.body = json.dumps(system, default=utils.json_serial)


class NodeList:

    def on_get(self, req, resp, system_id):
        init(system_id)
        nodes = sync.Node.get()
        jsonschema.validators.Draft4Validator(
            schema.node_list_get).validate(nodes)
        resp.body = utils.json.dumps(nodes, default=utils.json_serial)

    def on_post(self, req, resp, system_id):
        init(system_id)
        json_data = req.stream.read()
        node = utils.inflate(json_data, sync.Node(), schema.node_post)
        node.save()
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = utils.json.dumps(node, default=utils.json_serial)
        resp.status = falcon.HTTP_201


class Node:

    def on_get(self, req, resp, system_id, node_id):
        init(system_id)
        node = sync.Node.get(node_id)
        utils.obj_or_404(node)
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = json.dumps(node, default=utils.json_serial)


class NodeSync:

    def on_post(self, req, resp, system_id, node_id):
        init(system_id)
        node = sync.Node.get(node_id)
        utils.obj_or_404(node)
        node.sync()
