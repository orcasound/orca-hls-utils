"""
Improved HLS file locator for Orcasound archives.

This module provides utilities to efficiently locate HLS files in the
Orcasound S3 archive for a given location and timestamp using prefix filtering.
"""

import bisect
from datetime import datetime
from typing import List, Tuple, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from pytz import timezone


def get_folders_with_prefix(
    bucket: str, prefix: str, timestamp_prefix: str
) -> List[int]:
    """
    Get all folder names (as unix timestamps) that start with the given prefix.

    :param bucket: Name of the S3 bucket (e.g., 'audio-orcasound-net')
    :param prefix: Prefix to the HLS folders (e.g., 'rpi_bush_point/hls/')
    :param timestamp_prefix: First N digits of unix timestamp to filter by
    :return: Sorted list of folder names as integers
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator("list_objects_v2")

    full_prefix = f"{prefix}{timestamp_prefix}"
    kwargs = {"Bucket": bucket, "Prefix": full_prefix, "Delimiter": "/"}

    folders = []

    for page in paginator.paginate(**kwargs):
        try:
            common_prefixes = page["CommonPrefixes"]
            for prefix_obj in common_prefixes:
                folder_name = prefix_obj["Prefix"].split("/")[-2]
                try:
                    folders.append(int(folder_name))
                except ValueError:
                    # Skip non-numeric folder names
                    continue
        except KeyError:
            # No CommonPrefixes found
            break

    return sorted(folders)


def find_stream_for_timestamp(
    bucket: str,
    location: str,
    timestamp: int,
    prefix_length: int = 4
) -> Tuple[Optional[int], Optional[int]]:
    """
    Find the base stream folder and time offset for a given timestamp.

    Algorithm:
    1. Convert timestamp to unix (already done if passed as int)
    2. Use first N digits of unix timestamp to prefix-filter HLS bucket objects
    3. Also check prefix-1 to handle boundary cases where stream started earlier
    4. Get insertion index of unix timestamp in sorted prefix-filtered list
    5. Find base stream and calculate time offset from stream start

    :param bucket: Name of the S3 bucket (e.g., 'audio-orcasound-net')
    :param location: Hydrophone location (e.g., 'rpi_bush_point', 'rpi_north_sjc')
    :param timestamp: Unix timestamp to search for
    :param prefix_length: Number of leading digits to use for filtering (default: 4)
    :return: Tuple of (base_stream_timestamp, offset_seconds) or (None, None) if not found
    """
    # Get prefix for filtering
    timestamp_str = str(timestamp)
    timestamp_prefix = timestamp_str[:prefix_length]

    # Build the S3 prefix for this location's HLS files
    hls_prefix = f"{location}/hls/"

    # Get all folders matching the current prefix
    folders = get_folders_with_prefix(bucket, hls_prefix, timestamp_prefix)

    # Also check the previous prefix to handle boundary cases
    # (e.g., looking for 1767070800 but stream started at 1766999999)
    prev_prefix_int = int(timestamp_prefix) - 1
    if prev_prefix_int > 0:
        prev_prefix = str(prev_prefix_int)
        prev_folders = get_folders_with_prefix(bucket, hls_prefix, prev_prefix)
        folders.extend(prev_folders)
        folders = sorted(set(folders))  # Remove duplicates and sort

    if not folders:
        print(f"No folders found with prefix {timestamp_prefix} or {prev_prefix_int} for {location}")
        return None, None

    # Find the insertion point for the timestamp
    # bisect_right gives us the index where timestamp would be inserted
    # The stream we want is at index-1 (the folder that starts before our timestamp)
    insertion_index = bisect.bisect_right(folders, timestamp)

    if insertion_index == 0:
        print(f"Timestamp {timestamp} is before the first available stream {folders[0]}")
        return None, None

    # The base stream is the folder that starts before our timestamp
    base_stream = folders[insertion_index - 1]

    # Calculate offset in seconds
    offset_seconds = timestamp - base_stream

    return base_stream, offset_seconds


def datetime_to_unix(dt: datetime, tz_name: str = "US/Pacific") -> int:
    """
    Convert a datetime to unix timestamp.

    :param dt: datetime object (can be naive or aware)
    :param tz_name: Timezone name if dt is naive (default: US/Pacific)
    :return: Unix timestamp as integer
    """
    if dt.tzinfo is None:
        # Naive datetime, localize it
        tz = timezone(tz_name)
        dt = tz.localize(dt)

    return int(dt.timestamp())


def get_hls_url(
    bucket: str,
    location: str,
    timestamp: int
) -> Tuple[Optional[str], Optional[int]]:
    """
    Get the HLS URL and offset for a given location and timestamp.

    :param bucket: Name of the S3 bucket (e.g., 'audio-orcasound-net')
    :param location: Hydrophone location (e.g., 'rpi_bush_point', 'rpi_north_sjc')
    :param timestamp: Unix timestamp to search for
    :return: Tuple of (hls_url, offset_seconds) or (None, None) if not found
    """
    base_stream, offset_seconds = find_stream_for_timestamp(bucket, location, timestamp)

    if base_stream is None:
        return None, None

    hls_url = (
        f"https://s3-us-west-2.amazonaws.com/{bucket}/{location}/hls/"
        f"{base_stream}/live.m3u8"
    )

    return hls_url, offset_seconds


def list_streams_in_range(
    bucket: str,
    location: str,
    start_timestamp: int,
    end_timestamp: int
) -> List[int]:
    """
    List all stream folders in a time range.

    :param bucket: Name of the S3 bucket
    :param location: Hydrophone location
    :param start_timestamp: Start of time range (unix timestamp)
    :param end_timestamp: End of time range (unix timestamp)
    :return: Sorted list of stream folder timestamps in the range
    """
    # Get the prefix range we need to search
    start_prefix = str(start_timestamp)[:4]
    end_prefix = str(end_timestamp)[:4]

    all_folders = []
    hls_prefix = f"{location}/hls/"

    # Also check prefix before start_timestamp to catch streams that started earlier
    # but may still be active during our time range
    start_prefix_int = int(start_prefix)
    prev_start_prefix = str(start_prefix_int - 1) if start_prefix_int > 0 else None

    # If start and end are in the same prefix
    if start_prefix == end_prefix:
        folders = get_folders_with_prefix(bucket, hls_prefix, start_prefix)
        all_folders.extend(folders)
        # Also check previous prefix
        if prev_start_prefix:
            prev_folders = get_folders_with_prefix(bucket, hls_prefix, prev_start_prefix)
            all_folders.extend(prev_folders)
    else:
        # Search all prefixes in the range (including one before start)
        range_start = start_prefix_int - 1 if start_prefix_int > 0 else start_prefix_int
        for prefix_int in range(range_start, int(end_prefix) + 1):
            prefix = str(prefix_int)
            folders = get_folders_with_prefix(bucket, hls_prefix, prefix)
            all_folders.extend(folders)

    # Filter to only folders that overlap with the range and remove duplicates
    # A stream overlaps if it starts before end_timestamp
    all_folders = sorted(set(all_folders))
    filtered = [
        f for f in all_folders
        if f <= end_timestamp  # Stream started before or at end of range
    ]

    return filtered
