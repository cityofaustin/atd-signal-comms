from settings import STATUS_CODES

CONFIG = [
    {
        "device_type": "camera",
        "container": "view_395",
        "fields": {
            "ip_address": "field_638",
            "device_id": "field_947",
            "location_id": "field_642",
            "location_name": "field_211",
            "knack_id": "id",
            "signal_id": "field_199",
        },
    },
    {
        "device_type": "detector",
        "container": "view_1333",
        "fields": {
            "ip_address": "field_1570",
            "device_id": "field_1526",
            "location_id": "field_209",
            "location_name": "field_212",
            "knack_id": "id",
            "signal_id": "field_1579",
        },
    },
    {
        "device_type": "digital_message_sign",
        "container": "view_1564",
        "fields": {
            "ip_address": "field_1653",
            "device_id": "field_1639",
            "location_id": "field_732",
            "location_name": "field_211",
            "knack_id": "id",
        },
    },
    {
        "device_type": "cabinet_battery_backup",
        "container": "view_1567",
        "fields": {
            "ip_address": "field_3525",
            "device_id": "field_1789",
            # cabinets are not connected to a "locations" object record
            # there is no ATD_LOCATION_ID field. so just use device id
            "location_id": "field_1789",
            "location_name": "field_4128",
            "knack_id": "id",
            "signal_id": "field_1798",
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
    "status_code": {"type": "integer", "allowed": STATUS_CODES.keys()},
    "status_desc": {"type": "string", "allowed": STATUS_CODES.values()},
    "delay": {"type": "integer", "nullable": True},
    "timestamp": {
        "type": "string",
    },
    "device_type": {
        "type": "string",
    },
    "signal_id": {"type": ["integer", "string"], "nullable": True},
}
