#!/usr/bin/env python3
"""
Example usage of the improved HLS locator for Orcasound archives.

This script demonstrates how to use the hls_locator module to find and access
audio streams from specific timestamps.
"""

from datetime import datetime
from pytz import timezone

from orca_hls_utils.hls_locator import (
    find_stream_for_timestamp,
    get_hls_url,
    list_streams_in_range,
    datetime_to_unix,
)


def example_1_find_stream_from_timestamp():
    """Example: Find HLS stream URL for a specific timestamp"""
    print("\n" + "=" * 80)
    print("Example 1: Find HLS stream for a specific timestamp")
    print("=" * 80)

    # Using the example from the documentation
    BUCKET = "audio-orcasound-net"
    LOCATION = "rpi_bush_point"

    # Example timestamp from orcahello detection
    # From docs: timestamp: 2025-08-14T03:01:25.267833Z
    iso_ts = "2025-08-14T03:01:25.267833Z"
    dt = datetime.fromisoformat(iso_ts.replace('Z', '+00:00'))
    timestamp = int(dt.timestamp())

    print(f"\nSearching for stream at: {dt}")
    print(f"Unix timestamp: {timestamp}")

    base_stream, offset = find_stream_for_timestamp(BUCKET, LOCATION, timestamp)

    if base_stream:
        print(f"\n✓ Found stream!")
        print(f"  Base stream timestamp: {base_stream}")
        print(f"  Offset from stream start: {offset} seconds ({offset/60:.2f} minutes)")

        hls_url = f"https://s3-us-west-2.amazonaws.com/{BUCKET}/{LOCATION}/hls/{base_stream}/live.m3u8"
        print(f"  Stream URL: {hls_url}")

        # Calculate playlist index (assuming ~10 second segments)
        playlist_index = offset // 10
        print(f"  Approximate playlist index: {playlist_index}")
    else:
        print("\n✗ No stream found for this timestamp")


def example_2_list_daily_streams():
    """Example: List all streams available for a specific day"""
    print("\n" + "=" * 80)
    print("Example 2: List all streams for a specific day")
    print("=" * 80)

    BUCKET = "audio-orcasound-net"
    LOCATION = "rpi_bush_point"

    # Get all streams for August 14, 2025
    pst = timezone("US/Pacific")
    day_start = pst.localize(datetime(2025, 8, 14, 0, 0, 0))
    day_end = pst.localize(datetime(2025, 8, 14, 23, 59, 59))

    start_ts = int(day_start.timestamp())
    end_ts = int(day_end.timestamp())

    print(f"\nSearching for streams on: {day_start.date()}")
    print(f"Location: {LOCATION}")

    streams = list_streams_in_range(BUCKET, LOCATION, start_ts, end_ts)

    if streams:
        print(f"\n✓ Found {len(streams)} stream(s):")
        for i, stream_ts in enumerate(streams, 1):
            stream_dt = datetime.fromtimestamp(stream_ts, tz=pst)
            duration_start = stream_dt.strftime("%I:%M %p")

            # Calculate approximate duration until next stream or end of day
            if i < len(streams):
                duration = streams[i] - stream_ts
            else:
                duration = end_ts - stream_ts

            hours = duration // 3600
            minutes = (duration % 3600) // 60

            print(f"\n  Stream {i}:")
            print(f"    Start: {duration_start} ({stream_ts})")
            print(f"    Duration: ~{hours}h {minutes}m")
            print(f"    URL: https://s3-us-west-2.amazonaws.com/{BUCKET}/{LOCATION}/hls/{stream_ts}/live.m3u8")
    else:
        print("\n✗ No streams found for this day")


def example_3_check_stream_availability():
    """Example: Check if streams exist for a given time range"""
    print("\n" + "=" * 80)
    print("Example 3: Check stream availability")
    print("=" * 80)

    BUCKET = "audio-orcasound-net"
    LOCATION = "rpi_north_sjc"

    # Check different time ranges
    test_cases = [
        ("Example from docs", 1755154822),
        ("Recent past", datetime_to_unix(datetime.now(timezone("US/Pacific")))),
    ]

    for description, timestamp in test_cases:
        print(f"\n{description}:")
        print(f"  Timestamp: {timestamp}")

        dt = datetime.fromtimestamp(timestamp, tz=timezone("US/Pacific"))
        print(f"  Date/Time: {dt}")

        hls_url, offset = get_hls_url(BUCKET, LOCATION, timestamp)

        if hls_url:
            print(f"  ✓ Stream available")
            print(f"    URL: {hls_url}")
            print(f"    Offset: {offset}s ({offset/60:.2f}m)")
        else:
            print(f"  ✗ No stream available")


def example_4_convert_datetime():
    """Example: Convert datetime to unix timestamp for searching"""
    print("\n" + "=" * 80)
    print("Example 4: Convert datetime to unix timestamp")
    print("=" * 80)

    # Example: Looking for audio from a specific date/time
    pst = timezone("US/Pacific")
    target_dt = pst.localize(datetime(2025, 8, 14, 3, 1, 25))

    print(f"\nTarget date/time: {target_dt}")

    timestamp = datetime_to_unix(target_dt)
    print(f"Unix timestamp: {timestamp}")

    # Now use it to find stream
    BUCKET = "audio-orcasound-net"
    LOCATION = "rpi_bush_point"

    base_stream, offset = find_stream_for_timestamp(BUCKET, LOCATION, timestamp)

    if base_stream:
        print(f"\n✓ Stream found at {base_stream} with offset {offset}s")
    else:
        print("\n✗ No stream found")


def main():
    print("\n" + "=" * 80)
    print("Orcasound HLS Locator - Usage Examples")
    print("=" * 80)
    print("\nThis script demonstrates the improved algorithm for locating")
    print("HLS audio streams in the Orcasound S3 archive.")

    example_1_find_stream_from_timestamp()
    example_2_list_daily_streams()
    example_3_check_stream_availability()
    example_4_convert_datetime()

    print("\n" + "=" * 80)
    print("All examples complete!")
    print("=" * 80)
    print("\nNote: Stream availability depends on what's in the archive.")
    print("Future dates or gaps in recording will show no streams available.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
