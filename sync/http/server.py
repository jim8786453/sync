import falcon
from falcon_cors import CORS
import jsonschema

import sync

from sync.http import admin, errors, middleware, messaging


# Middleware.
cors = CORS(allow_all_origins=True, allow_all_headers=True,
            allow_all_methods=True)

api = falcon.API(middleware=[cors.middleware])

# Sync API.
api.add_route('/messages', messaging.MessageList())
api.add_route('/messages/pending', messaging.MessagePending())
api.add_route('/messages/next', messaging.MessageNext())
api.add_route('/messages/{message_id}', messaging.Message())


# Admin API.
api.add_route('/admin/networks', admin.NetworkList())
api.add_route('/admin/networks/{network_id}', admin.Network())
api.add_route('/admin/networks/{network_id}/nodes', admin.NodeList())
api.add_route('/admin/networks/{network_id}/nodes/{node_id}', admin.Node())
api.add_route('/admin/networks/{network_id}/nodes/{node_id}/sync', admin.NodeSync())


# Error handlers.
api.add_error_handler(
    sync.exceptions.DatabaseNotFoundError,
    errors.raise_http_not_found)

api.add_error_handler(
    sync.exceptions.InvalidIdError,
    errors.raise_http_not_found)

api.add_error_handler(
    jsonschema.exceptions.ValidationError,
    errors.raise_http_invalid_request)

api.add_error_handler(
    sync.exceptions.InvalidJsonError,
    errors.raise_http_invalid_request)

api.add_error_handler(
    sync.exceptions.InvalidOperationError,
    errors.raise_http_invalid_request)
