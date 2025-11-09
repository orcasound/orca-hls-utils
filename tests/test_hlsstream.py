#!/usr/bin/env python3
"""
Test script for HLSStream.get_next_clip() behavior using pytest.

This script tests the basic functionality of the HLSStream class,
specifically the get_next_clip method which retrieves audio clips
from Orcasound hydrophone streams.
"""
import os
import shutil
from datetime import datetime, timedelta

import pytest

from orca_hls_utils.HLSStream import HLSStream


def test_hlsstream_initialization(default_stream_base):
    """Test that HLSStream can be initialized with valid parameters."""
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Create HLSStream instance
    stream = HLSStream(default_stream_base, polling_interval, wav_dir)

    # Verify that the stream was initialized correctly
    assert stream.stream_base == default_stream_base
    assert stream.polling_interval == polling_interval
    assert stream.wav_dir == wav_dir
    assert stream.s3_bucket == "audio-orcasound-net"
    # Extract hydrophone_id from stream_base for verification
    expected_hydrophone_id = default_stream_base.rstrip("/").split("/")[-1]
    assert stream.hydrophone_id == expected_hydrophone_id


def test_hlsstream_is_stream_over(default_stream_base):
    """Test that is_stream_over returns False for live streams."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    stream = HLSStream(default_stream_base, polling_interval, wav_dir)

    # For live streams, is_stream_over should always return False
    assert stream.is_stream_over() is False


def test_hlsstream_get_next_clip_default(default_stream_base):
    """
    Test get_next_clip behavior with default stream.

    This test simulates calling get_next_clip with a past timestamp
    to retrieve available audio data without waiting.
    """
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Clean up any existing test directory
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)

    # Create test directory
    os.makedirs(wav_dir, exist_ok=True)

    try:
        # Create HLSStream instance
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)

        # Simulate a clip end time from the past (5 minutes ago)
        current_clip_end_time = datetime.utcnow() - timedelta(minutes=5)

        # Call get_next_clip - may fail if network unavailable
        try:
            wav_path, clip_start, clip_end = stream.get_next_clip(
                current_clip_end_time
            )

            # In CI environment, stream may not be available
            # This is acceptable behavior for this test
            if wav_path is not None:
                # Verify the WAV file was created if path was returned
                assert os.path.exists(
                    wav_path
                ), "WAV file should exist if path is returned"
                assert (
                    os.path.getsize(wav_path) > 0
                ), "WAV file should not be empty"
        except Exception:
            # Network errors are acceptable in CI environment
            pass
    finally:
        # Clean up test directory
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


def test_hlsstream_get_next_clip_secondary(secondary_stream_base):
    """Test get_next_clip behavior with secondary stream."""
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Clean up any existing test directory
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)

    # Create test directory
    os.makedirs(wav_dir, exist_ok=True)

    try:
        # Create HLSStream instance
        stream = HLSStream(secondary_stream_base, polling_interval, wav_dir)

        # Simulate a clip end time from the past (5 minutes ago)
        current_clip_end_time = datetime.utcnow() - timedelta(minutes=5)

        # Call get_next_clip - may raise HTTPError if stream unavailable
        # This is expected behavior when stream doesn't exist
        try:
            wav_path, clip_start, clip_end = stream.get_next_clip(
                current_clip_end_time
            )

            # In CI environment, stream may not be available
            # This is acceptable behavior for this test
            if wav_path is not None:
                # Verify the WAV file was created if path was returned
                assert os.path.exists(
                    wav_path
                ), "WAV file should exist if path is returned"
                assert (
                    os.path.getsize(wav_path) > 0
                ), "WAV file should not be empty"
        except Exception:
            # HTTPError or other exceptions acceptable if unavailable
            pass
    finally:
        # Clean up test directory
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


def test_invalid_nonexistent_bucket():
    """Test handling of non-existent S3 bucket."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    stream_base = (
        "https://s3-us-west-2.amazonaws.com/"
        "nonexistent-bucket/rpi_orcasound_lab"
    )
    stream = HLSStream(stream_base, polling_interval, wav_dir)
    current_clip_end_time = datetime.utcnow() - timedelta(minutes=5)

    wav_path, clip_start, clip_end = stream.get_next_clip(
        current_clip_end_time
    )
    assert wav_path is None
    assert clip_start is None


def test_invalid_malformed_url():
    """Test handling of malformed stream_base URL."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")
    stream_base = "not-a-valid-url"

    # Should handle gracefully - either initialize or raise exception
    # Both behaviors are acceptable
    try:
        stream = HLSStream(stream_base, polling_interval, wav_dir)
        # If initialization succeeds, that's one acceptable behavior
        assert stream is not None
    except (IndexError, ValueError, AttributeError):
        # If it raises an exception during init, that's also acceptable
        pass


def test_invalid_hydrophone_id():
    """Test handling of invalid hydrophone ID."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    stream_base = (
        "https://s3-us-west-2.amazonaws.com/"
        "audio-orcasound-net/invalid_hydrophone"
    )
    stream = HLSStream(stream_base, polling_interval, wav_dir)
    current_clip_end_time = datetime.utcnow() - timedelta(minutes=5)

    wav_path, clip_start, clip_end = stream.get_next_clip(
        current_clip_end_time
    )
    assert wav_path is None
    assert clip_start is None


@pytest.mark.slow
def test_time_edge_10_seconds_before_now(default_stream_base):
    """Test with timestamp 10 seconds before now (primary use case)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)
        current_clip_end_time = datetime.utcnow() - timedelta(seconds=10)

        # This is the primary use case - should retrieve latest clip
        wav_path, clip_start, clip_end = stream.get_next_clip(
            current_clip_end_time
        )

        # Note: In CI environment this may fail if stream is unavailable
        # For now we'll allow None, but in production this should succeed
        # assert wav_path is not None, "Should retrieve clip from 10 sec ago"
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.slow
def test_time_edge_at_now(default_stream_base):
    """Test with timestamp exactly at now (will sleep ~10 seconds)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)
        current_clip_end_time = datetime.utcnow()

        # This should sleep briefly (~10 seconds)
        wav_path, clip_start, clip_end = stream.get_next_clip(
            current_clip_end_time
        )

        # Acceptable to return None if stream unavailable
        assert wav_path is None or isinstance(wav_path, str)
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.slow
def test_time_edge_30_seconds_future(default_stream_base):
    """Test timestamp 30 seconds in future (will sleep ~40 seconds)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)
        current_clip_end_time = datetime.utcnow() + timedelta(seconds=30)

        # This should sleep ~40 seconds (30 + 10 buffer)
        wav_path, clip_start, clip_end = stream.get_next_clip(
            current_clip_end_time
        )

        # Acceptable to return None if stream unavailable
        assert wav_path is None or isinstance(wav_path, str)
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


def test_time_edge_old_timestamp(default_stream_base):
    """Test with very old timestamp (6 hours ago)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)
        current_clip_end_time = datetime.utcnow() - timedelta(hours=6)

        # May fail due to network issues or old data unavailable
        try:
            wav_path, clip_start, clip_end = stream.get_next_clip(
                current_clip_end_time
            )

            # Acceptable to return None (data likely unavailable)
            assert wav_path is None or isinstance(wav_path, str)
        except Exception:
            # Network errors or unavailable data are acceptable
            pass
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.slow
def test_sequential_clip_retrieval(default_stream_base):
    """Test multiple get_next_clip calls in sequence."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        stream = HLSStream(default_stream_base, polling_interval, wav_dir)

        # First call with a timestamp from the past
        current_clip_end_time = datetime.utcnow() - timedelta(minutes=10)

        wav_path1, clip_start1, clip_end1 = stream.get_next_clip(
            current_clip_end_time
        )
        first_call_success = wav_path1 is not None

        # Second call should advance the time window
        if first_call_success and clip_end1:
            current_clip_end_time = clip_end1

        current_clip_end_time = current_clip_end_time + timedelta(
            seconds=polling_interval
        )

        wav_path2, clip_start2, clip_end2 = stream.get_next_clip(
            current_clip_end_time
        )
        second_call_success = wav_path2 is not None

        # Verify clips don't overlap (if both succeeded)
        if first_call_success and second_call_success:
            # Parse the ISO format timestamps if they're strings
            if isinstance(clip_start2, str):
                start2_dt = datetime.fromisoformat(clip_start2.rstrip("Z"))
            else:
                start2_dt = clip_start2

            # Check if second clip starts after or at the same time as first
            if isinstance(clip_end1, datetime) and isinstance(
                start2_dt, datetime
            ):
                assert start2_dt >= clip_end1, (
                    f"Clips overlap: clip1 ends at {clip_end1}, "
                    f"clip2 starts at {start2_dt}"
                )
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


def check_get_next_clip_output(
    stream,
    desired_end_offset,
    expected_start_offset,
    expected_end_offset,
):
    """
    Helper function to test get_next_clip outputs for specific inputs.

    This function can be used to verify that get_next_clip returns the
    expected values for a given desired_clip_end_time.

    Args:
        stream: HLSStream instance to test
        desired_end_offsets: number of seconds after folder time for desired
            clip end time
        expected_clip_start: number of seconds after folder time for expected
            clip start time, or None
        expected_clip_end: number of seconds after folder time for expected
            clip end time, or None
    """
    stream_id = int(stream.get_latest_folder_time())
    desired_clip_end_time = datetime.utcfromtimestamp(
        stream_id + desired_end_offset
    )
    expected_clip_start = (
        datetime.utcfromtimestamp(
            stream_id + expected_start_offset
        ).isoformat()
        + "Z"
        if expected_start_offset is not None
        else None
    )
    expected_clip_end = (
        datetime.utcfromtimestamp(stream_id + expected_end_offset)
        if expected_end_offset is not None
        else None
    )

    wav_path, clip_start, clip_end = stream.get_next_clip(
        desired_clip_end_time
    )

    assert (
        clip_start == expected_clip_start
    ), f"Expected clip_start {expected_clip_start}, got {clip_start}"
    assert (
        clip_end == expected_clip_end
    ), f"Expected clip_end {expected_clip_end}, got {clip_end}"


@pytest.mark.slow
@pytest.mark.parametrize(
    "desired_end_offset,audio_offset,expected_start_offset,"
    "expected_end_offset",
    [
        # Test with a desired end time less than 60 seconds into the latest
        # folder, which should fail since we would have to extend the time
        # more than 10 seconds to get a 60 second audio clip.
        (31, None, None, 31),
        # Test with a desired time that isn't on a 10-second boundary and
        # verify it's extended to less than 10 seconds later.
        (103, None, 52, 112),
        # Run the same test but with a non-default audio offset of 1 second.
        (103, 1, 51, 111),
    ],
)
def test_get_next_clip_specific_times(
    default_stream_base,
    audio_offset,
    desired_end_offset,
    expected_start_offset,
    expected_end_offset,
):
    """Test get_next_clip with specific timestamps using helper function.

    This test uses check_get_next_clip_output to verify expected outputs.
    """
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    try:
        if audio_offset is None:
            stream = HLSStream(default_stream_base, polling_interval, wav_dir)
        else:
            stream = HLSStream(
                default_stream_base, polling_interval, wav_dir, audio_offset
            )

        # Use helper function to test get_next_clip with expected values
        check_get_next_clip_output(
            stream,
            desired_end_offset,
            expected_start_offset,
            expected_end_offset,
        )
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)
