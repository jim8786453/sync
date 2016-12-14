from datetime import datetime
import falcon
import json
import jsonschema

import sync

from sync.exceptions import InvalidJsonError


class PostData(object):
    pass


def inflate(json_data, obj, schema):
    if isinstance(json_data, (bytes, bytearray)):
        json_data = json_data.decode("utf-8")
    try:
        data = json.loads(json_data)
    except ValueError as ex:
        try:
            # Fails in Python 3.
            raise InvalidJsonError(ex.message)
        except:
            raise InvalidJsonError(ex)
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


def obj_or_404(obj):
    if obj is None:
        raise falcon.HTTPNotFound()
