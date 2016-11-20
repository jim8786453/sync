from datetime import datetime
import json
import jsonschema
import falcon

import sync

from sync.http import schema


def inflate(json_data, obj, schema):
    if isinstance(json_data, (bytes, bytearray)):
        json_data = json_data.decode("utf-8")
    data = json.loads(json_data)
    jsonschema.validators.Draft4Validator(schema).validate(data)
    for key in data.keys():
        setattr(obj, key, data[key])
    return obj


def json_serial(obj):
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    elif isinstance(obj, sync.Node):
        serial = obj.as_dict(with_id=True)
        return serial
    raise TypeError("Type not serializable: " + str(type(obj)))


class PostData(object):
    pass


class System:

    def on_post(self, req, resp):
        system_id = sync.generate_id()
        sync.http.server.init_storage(system_id, create_db=True)
        json_data = req.stream.read()
        system = inflate(json_data, sync.System(), schema.system_post)
        system.save()
        system = system.as_dict(with_id=True)
        nodes = sync.Node.get()
        system['nodes'] = nodes
        resp.body = json.dumps(system, default=json_serial)
        resp.status = falcon.HTTP_201

    def on_get(self, req, resp):
        system = sync.System.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.system_get).validate(system)
        nodes = sync.Node.get()
        system['nodes'] = nodes
        resp.body = json.dumps(system, default=json_serial)

    def on_patch(self, req, resp):
        system = sync.System.get()
        json_data = req.stream.read()
        system = inflate(json_data, system, schema.system_patch)
        system.save()
        system = sync.System.get().as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.system_get).validate(system)
        nodes = sync.Node.get()
        system['nodes'] = nodes
        resp.body = json.dumps(system, default=json_serial)


class NodeList:

    def on_post(self, req, resp):
        json_data = req.stream.read()
        node = inflate(json_data, sync.Node(), schema.node_post)
        node.save()
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = json.dumps(node, default=json_serial)
        resp.status = falcon.HTTP_201


class Node:

    def on_get(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        node = node.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.node_get).validate(node)
        resp.body = json.dumps(node, default=json_serial)


class NodeSend:

    def on_post(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        json_data = req.stream.read()
        data = inflate(json_data, PostData, schema.node_send_post)
        message = node.send(data.method, data.payload)
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=json_serial)


class NodeFetch:

    def on_post(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        message = node.fetch()
        if message is None:
            resp.status = falcon.HTTP_204
            return
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=json_serial)


class NodeAck:

    def on_post(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        json_data = req.stream.read()
        data = inflate(json_data, PostData, schema.node_ack_post)
        remote_id = getattr(data, 'remote_id', None)
        message = node.acknowledge(data.message_id, remote_id)
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=json_serial)


class NodeFail:

    def on_post(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        json_data = req.stream.read()
        data = inflate(json_data, PostData, schema.node_fail_post)
        reason = getattr(data, 'reason', None)
        message = node.fail(data.message_id, reason)
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=json_serial)


class NodeSync:

    def on_post(self, req, resp, node_id):
        node = sync.Node.get(node_id)
        if node is None:
            raise falcon.HTTPNotFound()
        node.sync()
