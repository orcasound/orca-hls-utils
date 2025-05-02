#! /usr/bin/env python3
from typing import List

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import m3u8
import s3fs


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
    bucket_list = [int(bucket) for bucket in bucket_list]
    start_index = 0
    end_index = len(bucket_list) - 1
    print(start_index)
    print(bucket_list[-1])
    print(int(start_time))
    while int(bucket_list[start_index]) < int(start_time):
        start_index += 1
        if start_index == len(bucket_list):
            break

    while int(bucket_list[end_index]) > int(end_time):
        end_index -= 1
    return bucket_list[start_index - 1 : end_index + 1]

def list_s3_files(bucket: str, prefix: str):
    """
    List all file keys under the given prefix from S3.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    return files

def download_s3_object(bucket: str, key: str, download_path: str):
    """
    Download an object from S3 to a local file.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3.download_file(bucket, key, download_path)

def verify_playlist(bucket: str, folder_prefix: str, verbose: bool = False):

    """
    Verify that the .ts files referenced in the playlist match the .ts files in the folder.
    """
    m3u8_key = folder_prefix + "live.m3u8"
    s3 = s3fs.S3FileSystem(anon=True)
    try:
        with s3.open(f"s3://{bucket}/{m3u8_key}", "r") as m3u8_file:
            playlist = m3u8.loads(m3u8_file.read())
    except Exception as e:
        print(f"[ERROR] Could not load playlist for {folder_prefix}: {e}")
        return

    referenced_ts = [seg.uri for seg in playlist.segments if seg.uri.endswith('.ts')]
    all_files = list_s3_files(bucket, folder_prefix)
    actual_ts = sorted(f.split("/")[-1].split(".")[0][4:] for f in all_files if f.endswith('.ts'))

    missing_files = set(referenced_ts) - set(actual_ts)
    extra_files = set(actual_ts) - set(referenced_ts)

    if not len(missing_files | extra_files):
        return False

    if verbose:
        if not missing_files and not extra_files:
            print(f"[OK] {folder_prefix}: Playlist matches .ts files.")
        else:
            if missing_files:
                print(f"[DISCREPANCY] {folder_prefix} missing files: {missing_files}")
            if extra_files:
                print(f"[DISCREPANCY] {folder_prefix} extra files: {extra_files}")

    return {
        "missing_files": [list(missing_files)],
        "extra_files": [list(extra_files)],
    }