#!/usr/bin/env python3
"""
Ping network devices and upload results to S3
"""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import logging
import os


import boto3
from pypgrest import Postgrest

from device import Device
from config import CONFIG
from settings import MAX_ATTEMPTS, NUM_WORKERS_DEFAULT
import utils

PGREST_JWT = os.getenv("PGREST_JWT")
PGREST_ENDPOINT = os.getenv("PGREST_ENDPOINT")
KNACK_APP_ID = os.getenv("KNACK_APP_ID")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET = os.getenv("BUCKET")


async def worker(queue):
    """
    Looping async queue worker.

    Fetches new Device tasks from queue, attempts to ping the device IP, and updates
    instance properties with the outcome.

    Args:
        queue (asyncio.Queue): asyncio.Queue with Device instances

    Returns:
        asyncio.CancelledError: If the worker is cancelled
    """
    while True:
        device = await queue.get()
        logger.debug(device)
        attempts = 0
        while attempts <= MAX_ATTEMPTS and device.status_code not in [1, -2, -3]:
            """Attempt to ping device until max atttempts is reached or staus code is:
             1 (online)
            -2 (invalid host name)
            -3 (unknown exception)

            In other words, retry on timeout up to max attempts
            """
            attempts += 1
            await device.ping()
        queue.task_done()


def construct_device(device, device_type, fields):
    """Create a Device instance

    Args:
        device (Knackpy.Record): A Knackpy.Record instance with device asset data
        device_type (str): The device type
        fields (dict): a dict of field mappings use to replace knack field names with
            humanized names (see config.py)

    Raises:
        ValueError: if ip_address or device_id fields are null or absent from Device kwargs

    Returns:
        Device: a Device instance
    """
    device_kwargs = {key: device.get(value) for key, value in fields.items()}
    device_kwargs["device_type"] = device_type
    return Device(**device_kwargs)


def get_device_records(container):
    """Fetch device asset records from Knack

    Args:
        container (str): a Knack object or view key

    Returns:
        list: a list of Knackpy.records
    """
    logger.debug("Getting records from knack-postgrest...")
    client = Postgrest(PGREST_ENDPOINT, token=PGREST_JWT)
    records = client.select(
        resource="knack",
        params={
            "select": "record",
            "app_id": f"eq.{KNACK_APP_ID}",
            "container_id": f"eq.{container}",
            "order": "updated_at",
        },
    )
    return [r.get("record") for r in records]


def log_results(results):
    results = Counter([d["status_desc"] for d in results])
    logger.info(dict(results))
    return


def validate_results(results):
    """Validate each result dict meets our schema definition.

        Serves as a test to ensure our schema is always adhered to. A validation error would
        likely occur if a schema change was made to the source Knack container which resulted
        in invalid/missing data.

    Args:
        results (list): list of dictionaries of the data to be upload to S3

    Raises:
        ValueError: if any dict fails validation.
    """
    v = utils.get_validator()
    for row in results:
        if not v.validate(row):
            raise ValueError(f"Row failed validation: {v.errors}")


async def main(*, device_type, env, workers):
    config = next(d for d in CONFIG if d["device_type"] == device_type)
    device_records = get_device_records(config["container"])

    devices = []
    for d in device_records:
        try:
            device = construct_device(d, device_type, config["fields"])
        except ValueError:
            # raised if required fields (ip_address, device_id) are missing or None
            continue
        devices.append(device)

    logger.debug(f"{len(devices)} devices to ping")

    # create FIFO queue and load Devices
    queue = asyncio.Queue()
    for d in devices:
        await queue.put(d)

    # create queue worker-tasks
    tasks = []
    for i in range(workers):
        task = asyncio.create_task(worker(queue))
        tasks.append(task)

    # run tasks until queue is empty
    await queue.join()

    # cancel worker tasks to clear them from memory
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # dictify-devices and validate data
    results = [d.__dict__ for d in devices]
    validate_results(results)
    log_results(results)

    # upload to S3
    logger.debug("Uploading JSON to S3...")
    now = datetime.now(timezone.utc)
    filename = utils.format_filename(env=env, device_type=device_type, dt=now)
    s3 = boto3.client("s3")
    s3.put_object(Body=json.dumps(results), Bucket=BUCKET, Key=filename)
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
        "-w",
        "--workers",
        type=int,
        default=NUM_WORKERS_DEFAULT,
        help=f"Number of concurrent workers",
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

    asyncio.run(main(device_type=args.device_type, env=args.env, workers=args.workers))
