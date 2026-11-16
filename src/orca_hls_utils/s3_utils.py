#! /usr/bin/env python3
from typing import List

import boto3
from botocore import UNSIGNED
from botocore.config import Config


# Borrowed pagination code from https://alexwlchan.net/2019/07/listing-s3-keys/
def get_all_folders(bucket: str, prefix: str) -> List[str]:
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}

    all_keys = []
    # Orcasound buckets are not predictably spaced throughout the time that
    # they've been up some are 2 hours, some hold 24 hours
    # So not making any assumptions

    for page in paginator.paginate(**kwargs):
        try:
            common_prefixes = page["CommonPrefixes"]
            prefixes = [
                prefix["Prefix"].split("/")[-2] for prefix in common_prefixes
            ]
            all_keys.extend(prefixes)

        except KeyError:
            print("No content returned")
            break

    return all_keys


def get_folders_between_timestamp(
    bucket_list: List[str], start_time: str, end_time: str
) -> List[int]:
    folders = sorted(int(bucket) for bucket in bucket_list)
    if not folders:
        return []

    # A session folder may start up to a day before its audio is requested.
    earliest = int(start_time) - 86400
    latest = int(end_time)
    return [folder for folder in folders if earliest <= folder <= latest]
