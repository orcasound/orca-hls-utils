#!/usr/bin/env python3
"""
Test script for fetching and processing audio data from Orcasound archive.

This script demonstrates the improved HLS file location algorithm for finding
audio streams based on location and timestamp.

Test parameters:
- Location: rpi_north_sjc
- Time range: 12/29/2025 9pm PST to 10pm PST
- Example URL: https://s3-us-west-2.amazonaws.com/audio-orcasound-net/rpi_north_sjc/hls/1755154822/live.m3u8
"""

from datetime import datetime
from pytz import timezone

from orca_hls_utils.hls_locator import (
    find_stream_for_timestamp,
    get_hls_url,
    list_streams_in_range,
    datetime_to_unix,
)


def main():
    # Configuration
    BUCKET = "audio-orcasound-net"
    LOCATION = "rpi_north_sjc"

    # Define time range: 12/29/2025 9pm PST to 10pm PST
    pst = timezone("US/Pacific")
    start_dt = pst.localize(datetime(2025, 12, 29, 21, 0, 0))  # 9pm PST
    end_dt = pst.localize(datetime(2025, 12, 29, 22, 0, 0))    # 10pm PST

    # Convert to unix timestamps
    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())

    print("=" * 80)
    print("Orcasound HLS Archive Test - Improved Locator Algorithm")
    print("=" * 80)
    print(f"\nLocation: {LOCATION}")
    print(f"Bucket: {BUCKET}")
    print(f"\nTime Range:")
    print(f"  Start: {start_dt} ({start_timestamp})")
    print(f"  End:   {end_dt} ({end_timestamp})")
    print()

    # Test 1: Find stream for start time
    print("-" * 80)
    print("Test 1: Find HLS stream for start time (9pm PST)")
    print("-" * 80)
    base_stream, offset = find_stream_for_timestamp(
        BUCKET, LOCATION, start_timestamp
    )

    if base_stream:
        print(f"✓ Found base stream: {base_stream}")
        print(f"  Offset from stream start: {offset} seconds ({offset/60:.2f} minutes)")

        hls_url = f"https://s3-us-west-2.amazonaws.com/{BUCKET}/{LOCATION}/hls/{base_stream}/live.m3u8"
        print(f"  HLS URL: {hls_url}")
    else:
        print("✗ No stream found for this timestamp")
    print()

    # Test 2: Get HLS URL directly
    print("-" * 80)
    print("Test 2: Get HLS URL using convenience function")
    print("-" * 80)
    hls_url, offset = get_hls_url(BUCKET, LOCATION, start_timestamp)

    if hls_url:
        print(f"✓ HLS URL: {hls_url}")
        print(f"  Offset: {offset} seconds")
    else:
        print("✗ No HLS URL found")
    print()

    # Test 3: List all streams in the time range
    print("-" * 80)
    print("Test 3: List all streams in time range (9pm-10pm PST)")
    print("-" * 80)
    streams = list_streams_in_range(BUCKET, LOCATION, start_timestamp, end_timestamp)

    if streams:
        print(f"✓ Found {len(streams)} stream(s) in range:")
        for i, stream_ts in enumerate(streams, 1):
            stream_dt = datetime.fromtimestamp(stream_ts, tz=pst)
            print(f"  {i}. {stream_ts} - {stream_dt}")
            print(f"     URL: https://s3-us-west-2.amazonaws.com/{BUCKET}/{LOCATION}/hls/{stream_ts}/live.m3u8")
    else:
        print("✗ No streams found in time range")
    print()

    # Test 4: Test with the example URL timestamp
    print("-" * 80)
    print("Test 4: Verify example URL timestamp (1755154822)")
    print("-" * 80)
    example_timestamp = 1755154822
    example_dt = datetime.fromtimestamp(example_timestamp, tz=pst)
    print(f"Example timestamp: {example_timestamp}")
    print(f"Corresponds to: {example_dt}")

    base_stream, offset = find_stream_for_timestamp(
        BUCKET, LOCATION, example_timestamp
    )

    if base_stream:
        print(f"✓ Base stream for example: {base_stream}")
        print(f"  Offset: {offset} seconds")

        if base_stream == example_timestamp:
            print("  Note: Example timestamp is itself a stream start time")
        else:
            print(f"  Example URL should reference stream {base_stream}, not {example_timestamp}")
    else:
        print("✗ No stream found")
    print()

    # Test 5: Algorithm demonstration with arbitrary timestamp
    print("-" * 80)
    print("Test 5: Algorithm demonstration")
    print("-" * 80)
    print("Algorithm steps:")
    print("1. Convert timestamp to unix (already done)")
    print(f"2. Use first 4 digits ({str(start_timestamp)[:4]}) to prefix-filter S3 objects")
    print("3. Find insertion index in sorted list")
    print("4. Calculate offset from base stream")
    print()
    print("This approach is more efficient than scanning all folders,")
    print("especially for large archives with thousands of stream folders.")
    print()

    print("=" * 80)
    print("Test Complete")
    print("=" * 80)


if __name__ == "__main__":
    main()
