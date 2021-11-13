#!/usr/bin/env python3
"""
Download comm status files and publish to Socrata
"""
import argparse
from datetime import datetime, timezone, timedelta
import json
import logging
import os

import boto3
import sodapy

from config import DATE_FORMAT_FILE
from settings import SOCRATA_RESOURCE_ID
import utils

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET")
SOCRATA_USER = os.getenv("SOCRATA_USER")
SOCRATA_PW = os.getenv("SOCRATA_PW")
SOCRATA_TOKEN = os.getenv("SOCRATA_TOKEN")


def get_socrata_client():
    return sodapy.Socrata(
        "data.austintexas.gov",
        SOCRATA_TOKEN,
        username=SOCRATA_USER,
        password=SOCRATA_PW,
        timeout=60,
    )


def get_files_to_download(bucket_prefixes: list, files_todo: list, client) -> list:
    """Retrieve a list of all file paths from S3 which exist within the provided
        files_todo list.

    Files are stored in S3 under the pattern <env>/<device_type>/<year>/<month>. To find a
    file for a specific day in S3, we first need to identify its month subdirectory, then
    we can list files in that subdirectory to see if the file is present.

    Args:
        bucket_prefixes (list): a list of prefix strings which will be used to filter
            objects in the bucket. This will be a lists of year/month paths which encompass
            the entire range of dates requested.
        files_todo (list): a list of S3 object path strings
        client (botocore.client.S3): the boto client (i could find the right import path to include a type hint :/ )

    Returns:
        list: a list of S3 object paths which exist in both files_todo and the S3 bucket.
    """
    paginator = client.get_paginator("list_objects")
    files = []

    for year_month_prefix in bucket_prefixes:
        page_iterator = paginator.paginate(Bucket=BUCKET, Prefix=year_month_prefix)
        for page in page_iterator:
            contents = page.get("Contents", [])
            files.extend([obj["Key"] for obj in contents if obj["Key"] in files_todo])
    return files


def date_range(date_min: datetime, date_max: datetime) -> list:
    """
    Args:
        date_min (datetime.datetime): the minimum date in the range
        date_max (datetime.datetime): the maximum date in the range

    Returns:
        list: a list of datetime objects included in the given min/max dates
    """
    range_delta = date_max - date_min
    return [date_min + timedelta(days=i) for i in range(range_delta.days + 1)]


def parse_date(
    date_str: str, fmt: str, tzinfo: timezone = timezone.utc
) -> datetime:
    """
    Args:
        date_str (str): an input date matching the given input format
        fmt (str, optional): The formatting template.
        tzinfo (datetime.timezone, optional): The timezone of the input date. Defaults to timezone.utc.

    Raises:
        ValueError: If unable to parse date string

    Returns:
        datetime.datetime: The datetime object.
    """
    try:
        return datetime.strptime(date_str, fmt).replace(tzinfo=tzinfo)
    except ValueError:
        raise ValueError(f"Unable to parse date '{date_str}' as YYYY-MM-DD")
    except TypeError:
        raise ValueError(f"Invalid date string input: {type(date_str)}")


def get_bucket_prefixes(device_type: str, env: str, dates_todo: list) -> list:
    """Generate a list of "prefixes" to be used to filter objects in the S3 bucket.

    Files are stored in S3 under the pattern <env>/<device_type>/<year>/<month>. To find a
    file for a specific day in S3, we first need to identify it's month subdirectory, then
    we can list files in that subdirectory to see if the file is present.

    Args:
        device_type (str): The type of device
        env (str): The environment (dev or prod)
        dates_todo (list): A list of datetime objects

    Returns:
        list: a list of bucket prefixes that encompass all files for the requested range.s
    """
    prefixes = [f"{env}/{device_type}/{d.year}/{d.month}" for d in dates_todo]
    prefixes = list(set(prefixes))
    prefixes.sort()
    return prefixes


def download_file(client, key):
    """Download a file from S3

    Args:
        client (botocore.client.S3): the boto client
        key (str): the S3 object key
    Returns:
        bytes: the encoded file data
    """
    logger.debug(f"Downloading {key}...")
    response = client.get_object(Bucket=BUCKET, Key=key)
    return response["Body"].read()


def main(device_type, env, start, end):
    date_min = parse_date(start, DATE_FORMAT_FILE)
    date_max = parse_date(end, DATE_FORMAT_FILE)
    dates_todo = date_range(date_min, date_max)

    logger.debug(
        f"Processing {dates_todo[0].strftime(DATE_FORMAT_FILE)} to {dates_todo[-1].strftime(DATE_FORMAT_FILE)}"
    )

    # generate a list of file names that fall within the given range
    files_todo = [
        utils.format_filename(env=env, device_type=device_type, dt=dt)
        for dt in dates_todo
    ]

    # generate a list of bucket prefixes (folders) that would contain files in the range
    bucket_prefixes = get_bucket_prefixes(device_type, env, dates_todo)

    logger.debug(
        f"Checking for S3 objects from {bucket_prefixes[0]} to {bucket_prefixes[-1]}"
    )

    # retrieve from S3 a list of actually existing objects which meet our date criteria
    client = boto3.client("s3")
    files_to_download = get_files_to_download(bucket_prefixes, files_todo, client)

    logger.debug(f"{len(files_to_download)} found in bucket")

    if not files_to_download:
        return

    # publish to socrata
    resource_id = SOCRATA_RESOURCE_ID[env]

    socrata_client = get_socrata_client()

    for key in files_to_download:
        json_file = download_file(client, key)
        rows = json.loads(json_file.decode())
        logger.debug(f"Upserting {key} to Socrata...")
        socrata_client.upsert(resource_id, rows)

    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        dest="device_type",
        type=str,
        choices=utils.supported_device_types(),
        help=f"The name of the device type",
    )

    parser.add_argument(
        "-e",
        "--env",
        type=str,
        choices=["dev", "prod"],
        default="dev",
        help=f"Environment",
    )

    parser.add_argument(
        "--start",
        type=str,
        default=datetime.now(timezone.utc).strftime(DATE_FORMAT_FILE),
        help=f"Date (in UTC) of earliest records to be fetched (YYYY-MM-DD). Defaults to today",
    )

    parser.add_argument(
        "--end",
        type=str,
        default=datetime.now(timezone.utc).strftime(DATE_FORMAT_FILE),
        help=f"End (in UTC) of oldest records to be fetched YYYY-MM-DD). Defaults to today",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help=f"Sets logger to DEBUG level",
    )

    args = parser.parse_args()

    logger = utils.get_logger(
        __name__,
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    main(args.device_type, args.env, args.start, args.end)
