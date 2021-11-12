DATE_FORMAT_FILE = "%Y-%m-%d"
DATE_FORMAT_SOCRATA = "%Y-%m-%dT%H:%M:%S"
STATUS_CODES = {0: "no_attempts", 1: "online", -1: "timeout", -2: "invalid_hostname", -3: "unknown_error", }

CONFIG = [
    {
        "device_type": "camera",
        "container": "view_3059",
        "fields": {
            "ip_address": "field_638",
            "device_id": "field_947",
            "location_id": "field_732",
            "location_name": "field_211",
            "knack_id": "id",
        },
    },
    {
        "device_type": "detector",
        "container": "view_3060",
        "fields": {
            "ip_address": "field_1570",
            "device_id": "field_1526",
            "location_id": "field_732",
            "location_name": "field_211",
            "knack_id": "id",
        },
    },
]


SCHEMA = {
    "id": {
        "type": "string",
    },
    "ip_address": {
        "type": "string",
    },
    "device_id": {
        "type": "integer",
    },
    "knack_id": {
        "type": "string",
    },
    "location_name": {"type": "string", "nullable": True},
    "location_id": {"type": "string", "nullable": True},
    "status_code": {
        "type": "integer",
        "allowed": STATUS_CODES.keys()
    },
    "status_desc": {
        "type": "string",
        "allowed": STATUS_CODES.values()
    },
    "delay": {"type": "integer", "nullable": True},
    "timestamp": {
        "type": "string",
    },
    "device_type": {
        "type": "string",
    },
}
