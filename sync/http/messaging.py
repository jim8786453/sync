import json
import jsonschema
import falcon

import sync

from sync import schema
from sync.http import utils
from sync.storage import init_storage


_HEADER_NETWORK_ID = 'X-Sync-Network-Id'
_HEADER_NODE_ID = 'X-Sync-Node-Id'


def handle_headers(req, resp, resource, params):
    network_id = req.get_header(_HEADER_NETWORK_ID)
    node_id = req.get_header(_HEADER_NODE_ID)
    if network_id is None:
        raise falcon.HTTPMissingHeader(_HEADER_NETWORK_ID)
    if node_id is None:
        raise falcon.HTTPMissingHeader(_HEADER_NODE_ID)
    init_storage(network_id, create_db=False)
    node = sync.Node.get(node_id)
    utils.obj_or_404(node)
    params['node'] = node


@falcon.before(handle_headers)
class MessageList:

    def on_post(self, req, resp, node):
        json_data = req.stream.read()
        data = utils.inflate(json_data, utils.PostData,
                             schema.message_create)
        method = data.method
        payload = data.payload
        record_id = getattr(data, 'record_id', None)
        remote_id = getattr(data, 'remote_id', None)
        message = node.send(method, payload, record_id, remote_id)
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=utils.json_serial)


@falcon.before(handle_headers)
class MessagePending:

    def on_get(self, req, resp, node):
        result = node.has_pending()
        jsonschema.validators.Draft4Validator(
            schema.message_pending_get).validate(result)
        resp.body = json.dumps(result, default=utils.json_serial)


@falcon.before(handle_headers)
class MessageNext:

    def on_post(self, req, resp, node):
        message = node.fetch()
        if message is None:
            resp.status = falcon.HTTP_204
            return
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=utils.json_serial)


@falcon.before(handle_headers)
class Message:

    def on_patch(self, req, resp, message_id, node):
        utils.obj_or_404(node)
        json_data = req.stream.read()
        data = utils.inflate(json_data, utils.PostData, schema.message_update)
        if data.success:
            remote_id = getattr(data, 'remote_id', None)
            message = node.acknowledge(message_id, remote_id)
        else:
            reason = getattr(data, 'reason', None)
            message = node.fail(message_id, reason)
        message = message.as_dict(with_id=True)
        jsonschema.validators.Draft4Validator(
            schema.message_get).validate(message)
        resp.body = json.dumps(message, default=utils.json_serial)
