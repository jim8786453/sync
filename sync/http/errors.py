import falcon


def raise_http_not_found(ex, req, resp, params):
    raise falcon.HTTPNotFound()


def raise_http_invalid_request(ex, req, resp, params):
    message = ''
    try:
        message = str(ex.message)
    except:
        message = str(ex)
    raise falcon.HTTPBadRequest((
        'Payload failed validation',
        message))
