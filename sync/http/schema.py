
json_schema = {
    "id": "http://json-schema.org/draft-04/schema#",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Core schema meta-schema",
    "definitions": {
        "schemaArray": {
            "type": "array",
            "minItems": 1,
            "items": {"$ref": "#"}
        },
        "positiveInteger": {
            "type": "integer",
            "minimum": 0
        },
        "positiveIntegerDefault0": {
            "allOf": [
                {"$ref": "#/definitions/positiveInteger"},
                {"default": 0}]
        },
        "simpleTypes": {
            "enum": [
                "array",
                "boolean",
                "integer",
                "null",
                "number",
                "object",
                "string"
            ]
        },
        "stringArray": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "uniqueItems": True
        }
    },
    "type": "object",
    "properties": {
        "id": {
            "type": "string",
            "format": "uri"
        },
        "$schema": {
            "type": "string",
            "format": "uri"
        },
        "title": {
            "type": "string"
        },
        "description": {
            "type": "string"
        },
        "default": {},
        "multipleOf": {
            "type": "number",
            "minimum": 0,
            "exclusiveMinimum": True
        },
        "maximum": {
            "type": "number"
        },
        "exclusiveMaximum": {
            "type": "boolean",
            "default": False
        },
        "minimum": {
            "type": "number"
        },
        "exclusiveMinimum": {
            "type": "boolean",
            "default": False
        },
        "maxLength": {"$ref": "#/definitions/positiveInteger"},
        "minLength": {"$ref": "#/definitions/positiveIntegerDefault0"},
        "pattern": {
            "type": "string",
            "format": "regex"
        },
        "additionalItems": {
            "anyOf": [
                {"type": "boolean"},
                {"$ref": "#"}
            ],
            "default": {}
        },
        "items": {
            "anyOf": [
                {"$ref": "#"},
                {"$ref": "#/definitions/schemaArray"}
            ],
            "default": {}
        },
        "maxItems": {"$ref": "#/definitions/positiveInteger"},
        "minItems": {"$ref": "#/definitions/positiveIntegerDefault0"},
        "uniqueItems": {
            "type": "boolean",
            "default": False
        },
        "maxProperties": {
            "$ref": "#/definitions/positiveInteger"
        },
        "minProperties": {
            "$ref": "#/definitions/positiveIntegerDefault0"
        },
        "required": {"$ref": "#/definitions/stringArray"},
        "additionalProperties": {
            "anyOf": [
                {"type": "boolean"},
                {"$ref": "#"}
            ],
            "default": {}
        },
        "definitions": {
            "type": "object",
            "additionalProperties": {"$ref": "#"},
            "default": {}
        },
        "properties": {
            "type": "object",
            "additionalProperties": {"$ref": "#"},
            "default": {}
        },
        "patternProperties": {
            "type": "object",
            "additionalProperties": {"$ref": "#"},
            "default": {}
        },
        "dependencies": {
            "type": "object",
            "additionalProperties": {
                "anyOf": [
                    {"$ref": "#"},
                    {"$ref": "#/definitions/stringArray"}
                ]
            }
        },
        "enum": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True
        },
        "type": {
            "anyOf": [
                {"$ref": "#/definitions/simpleTypes"},
                {
                    "type": "array",
                    "items": {"$ref": "#/definitions/simpleTypes"},
                    "minItems": 1,
                    "uniqueItems": True
                }
            ]
        },
        "allOf": {"$ref": "#/definitions/schemaArray"},
        "anyOf": {"$ref": "#/definitions/schemaArray"},
        "oneOf": {"$ref": "#/definitions/schemaArray"},
        "not": {"$ref": "#"}
    },
    "dependencies": {
        "exclusiveMaximum": ["maximum"],
        "exclusiveMinimum": ["minimum"]
    },
}

system_get = {
    "$schema": "http://json-schema.org/draft-04/schema#system_get",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "fetch_before_send": {
            "type": "boolean"
        },
        "schema": json_schema
    },
    "required": [
        "name",
        "id",
        "fetch_before_send",
        "schema"
    ]
}

system_post = {
    "$schema": "http://json-schema.org/draft-04/schema#system_post",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "fetch_before_send": {
            "type": "boolean"
        },
        "schema": json_schema
    },
    "required": [
        "name",
        "fetch_before_send",
        "schema"
    ]
}

system_patch = {
    "$schema": "http://json-schema.org/draft-04/schema#system_patch",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "fetch_before_send": {
            "type": "boolean"
        },
        "schema": json_schema
    },
    "required": [
        "id"
    ]
}

node_get = {
    "$schema": "http://json-schema.org/draft-04/schema#node_get",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "create": {
            "type": "boolean"
        },
        "read": {
            "type": "boolean"
        },
        "update": {
            "type": "boolean"
        },
        "delete": {
            "type": "boolean"
        },
    },
    "required": [
        "name",
        "id",
        "create",
        "read",
        "update",
        "delete"
    ]
}

node_post = {
    "$schema": "http://json-schema.org/draft-04/schema#node_post",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "create": {
            "type": "boolean"
        },
        "read": {
            "type": "boolean"
        },
        "update": {
            "type": "boolean"
        },
        "delete": {
            "type": "boolean"
        },
    },
    "required": [
        "name",
        "create",
        "read",
        "update",
        "delete"
    ]
}

node_patch = {
    "$schema": "http://json-schema.org/draft-04/schema#node_patch",
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "id": {
            "type": "string"
        },
        "create": {
            "type": "boolean"
        },
        "read": {
            "type": "boolean"
        },
        "update": {
            "type": "boolean"
        },
        "delete": {
            "type": "boolean"
        },
    },
    "required": [
        "id",
    ]
}

node_send_post = {
    "$schema": "http://json-schema.org/draft-04/schema#node_send",
    "type": "object",
    "properties": {
        "method": {
            "type": "string"
        },
        "payload": {
            "type": "object"
        },
        "record_id": {
            "type": "string"
        },
        "remote_id": {
            "type": "string"
        },
    },
    "required": [
        "method",
        "payload"
    ]
}

node_ack_post = {
    "$schema": "http://json-schema.org/draft-04/schema#node_ack",
    "type": "object",
    "properties": {
        "message_id": {
            "type": "string"
        },
        "remote_id": {
            "type": "string"
        },
    },
    "required": [
        "message_id"
    ]
}

node_fail_post = {
    "$schema": "http://json-schema.org/draft-04/schema#node_fail",
    "type": "object",
    "properties": {
        "message_id": {
            "type": "string"
        },
        "reason": {
            "type": "string"
        },
    },
    "required": [
        "message_id"
    ]
}

message_get = {
  "$schema": "http://json-schema.org/draft-04/schema#message_get",
  "type": "object",
  "properties": {
    "origin_id": {
      "type": ["string", "null"]
    },
    "remote_id": {
      "type": ["string", "null"]
    },
    "id": {
      "type": "string"
    },
    "parent_id": {
      "type": ["string", "null"]
    },
    "state": {
      "type": "string"
    },
    "destination_id": {
      "type": ["string", "null"]
    },
    "record_id": {
      "type": "string"
    },
    "payload": {
      "type": "object"
    },
    "method": {
      "type": "string"
    }
  },
  "required": [
    "origin_id",
    "remote_id",
    "id",
    "parent_id",
    "state",
    "destination_id",
    "record_id",
    "payload",
    "method"
  ]
}
