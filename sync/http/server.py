import falcon
import jsonschema

import sync

from sync.http import admin, errors, middleware, messaging

# Middleware.
api = falcon.API(middleware=[
    middleware.Sync()])


# Admin API.
api.add_route('/systems', admin.SystemList())
api.add_route('/systems/{system_id}', admin.System())
api.add_route('/systems/{system_id}/nodes', admin.NodeList())
api.add_route('/systems/{system_id}/nodes/{node_id}', admin.Node())
api.add_route('/systems/{system_id}/nodes/{node_id}/sync', admin.NodeSync())


# Message API.
api.add_route('/messages', messaging.MessageList())
api.add_route('/messages/pending', messaging.MessagePending())
api.add_route('/messages/next', messaging.MessageNext())
api.add_route('/messages/{message_id}', messaging.Message())


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
