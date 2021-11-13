import logging
import sys

import cerberus
from config import CONFIG, DATE_FORMAT_FILE, SCHEMA


def get_logger(name, level):
    """Return a module logger that streams to stdout"""
    logger = logging.getLogger(name)
    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s: %(message)s")
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger



def get_validator():
    return cerberus.Validator(SCHEMA, require_all=True)


def supported_device_types():
    """Generate a list of device types from the config file.

    Returns:
        list: A list of device type strings
    """
    return [d["device_type"] for d in CONFIG]


def format_filename(*, device_type, env, dt):
    """Format a file name for S3 storage

    Args:
        env (str): the environment - test or prod
        device_type (str): the device type name

    Returns:
        str: a path + filename to be used as the S3 bucket path
    """
    return f"{env}/{device_type}/{dt.year}/{dt.month}/{dt.strftime(DATE_FORMAT_FILE)}.json"
