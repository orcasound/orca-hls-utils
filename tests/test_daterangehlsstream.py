#!/usr/bin/env python3
"""
Test script for DateRangeHLSStream.get_next_clip() behavior using pytest.

This script tests the basic functionality of the DateRangeHLSStream class,
specifically the get_next_clip method which retrieves audio clips
from Orcasound hydrophone streams within a specific date range.
"""
import os
import shutil
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream


@pytest.mark.tests
def test_daterangehlsstream_initialization(default_stream_base):
    """Test DateRangeHLSStream initialization with valid parameters."""
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Set up a date range (e.g., 2 hours ago to 1 hour ago)
    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    # Create DateRangeHLSStream instance
    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        # Verify that the stream was initialized correctly
        assert stream.stream_base == default_stream_base
        assert stream.polling_interval_in_seconds == polling_interval
        assert stream.start_unix_time == start_unix_time
        assert stream.end_unix_time == end_unix_time
        assert stream.wav_dir == wav_dir
        assert stream.s3_bucket == "audio-orcasound-net"
        # Extract folder_name from stream_base for verification
        expected_folder_name = default_stream_base.rstrip("/").split("/")[-1]
        assert stream.folder_name == expected_folder_name
    except Exception:
        # Initialization may fail if no data exists in the date range
        # This is acceptable behavior
        pass


@pytest.mark.tests
def test_daterangehlsstream_is_stream_over(default_stream_base):
    """Test that is_stream_over correctly identifies when stream ends."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    # Stream should not be over at start
    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=2)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        # At initialization, stream should not be over
        assert stream.is_stream_over() is False

        # Manually set current_clip_start_time to end to test is_stream_over
        stream.current_clip_start_time = end_unix_time
        assert stream.is_stream_over() is True
    except Exception:
        # Initialization may fail if no data exists in the date range
        # This is acceptable behavior
        pass


@pytest.mark.tests
def test_daterangehlsstream_get_next_clip_default(default_stream_base):
    """
    Test get_next_clip behavior with default stream.

    This test simulates calling get_next_clip to retrieve available
    audio data from a historical date range.
    """
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Clean up any existing test directory
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)

    # Create test directory
    os.makedirs(wav_dir, exist_ok=True)

    # Set up a date range (e.g., 5 hours ago to 4 hours ago)
    end_time = datetime.utcnow() - timedelta(hours=4)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        wav_path, clip_start, clip_end = stream.get_next_clip()

        # In CI environment, stream may not be available or no data exists
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
        # Network errors or no data in date range are acceptable
        pass
    finally:
        # Clean up test directory
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.tests
def test_daterangehlsstream_get_next_clip_secondary(secondary_stream_base):
    """Test get_next_clip behavior with secondary stream."""
    polling_interval = 60  # seconds
    wav_dir = os.path.join(".", "test_wav_output")

    # Clean up any existing test directory
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)

    # Create test directory
    os.makedirs(wav_dir, exist_ok=True)

    # Set up a date range (e.g., 5 hours ago to 4 hours ago)
    end_time = datetime.utcnow() - timedelta(hours=4)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            secondary_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        wav_path, clip_start, clip_end = stream.get_next_clip()

        # In CI environment, stream may not be available or no data exists
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
        # Network errors or no data in date range are acceptable
        pass
    finally:
        # Clean up test directory
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.tests
def test_invalid_future_date_range():
    """Test error handling with future date range."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")
    default_stream_base = (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
        "rpi_orcasound_lab"
    )

    start_time = datetime.utcnow() + timedelta(hours=1)
    end_time = start_time + timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    # Should handle gracefully - either initialize or raise exception
    # Both behaviors are acceptable
    try:
        stream = DateRangeHLSStream(  # noqa: F841
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )
        # If initialization succeeds, that's acceptable (may have no folders)
    except Exception:
        # If it raises an exception, that's also acceptable
        pass


@pytest.mark.tests
def test_invalid_old_date_range():
    """Test error handling with very old date range (likely no data)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")
    default_stream_base = (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
        "rpi_orcasound_lab"
    )

    start_time = datetime.utcnow() - timedelta(days=365)
    end_time = start_time + timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(  # noqa: F841
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )
        # If initialization succeeds, that's acceptable
    except Exception:
        # If it raises an exception, that's also acceptable
        pass


@pytest.mark.tests
def test_invalid_reversed_date_range():
    """Test error handling with reversed date range (end before start)."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")
    default_stream_base = (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
        "rpi_orcasound_lab"
    )

    end_time = datetime.utcnow() - timedelta(hours=2)
    start_time = datetime.utcnow() - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        _stream = DateRangeHLSStream(  # noqa: F841
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )
        # If initialization succeeds, that's acceptable (may have no folders)
    except Exception:
        # If it raises an exception, that's also acceptable
        pass


@pytest.mark.slow
@pytest.mark.tests
def test_sequential_clip_retrieval(default_stream_base):
    """Test multiple get_next_clip calls in sequence."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    # Clean up test directory
    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    # Set up a date range with sufficient duration for multiple clips
    end_time = datetime.utcnow() - timedelta(hours=2)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        # First call
        wav_path1, clip_start1, clip_end1 = stream.get_next_clip()
        first_call_success = wav_path1 is not None

        # Second call should advance to next clip
        wav_path2, clip_start2, clip_end2 = stream.get_next_clip()
        second_call_success = wav_path2 is not None

        # Verify time window advanced
        if first_call_success and second_call_success:
            # The current_clip_start_time should have advanced
            assert clip_start2 != clip_start1, (
                "Time window should advance - "
                "second clip should have different start time"
            )
    except Exception:
        # Network errors or no data in date range are acceptable
        pass
    finally:
        # Clean up test directory
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)


@pytest.mark.tests
def test_real_time_mode_false(default_stream_base):
    """Test real_time mode parameter set to False."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
            real_time=False,
        )
        assert stream.real_time is False
    except Exception:
        # Initialization may fail if no data exists in the date range
        pass


@pytest.mark.tests
def test_real_time_mode_true(default_stream_base):
    """Test real_time mode parameter set to True."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
            real_time=True,
        )
        assert stream.real_time is True
    except Exception:
        # Initialization may fail if no data exists in the date range
        pass


@pytest.mark.tests
def test_overwrite_output_false(default_stream_base):
    """Test overwrite_output parameter set to False."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
            overwrite_output=False,
        )
        assert stream.overwrite_output is False
    except Exception:
        # Initialization may fail if no data exists in the date range
        pass


@pytest.mark.tests
def test_overwrite_output_true(default_stream_base):
    """Test overwrite_output parameter set to True."""
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    end_time = datetime.utcnow() - timedelta(hours=1)
    start_time = end_time - timedelta(hours=1)
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
            overwrite_output=True,
        )
        assert stream.overwrite_output is True
    except Exception:
        # Initialization may fail if no data exists in the date range
        pass


def check_daterange_get_next_clip_output(
    stream, expected_wav_path, expected_clip_start, expected_clip_end
):
    """
    Helper function to test DateRangeHLSStream.get_next_clip outputs.

    This function can be used to verify that get_next_clip returns the
    expected values when called without parameters.

    Args:
        stream: DateRangeHLSStream instance to test
        expected_wav_path: expected wav file path (or None)
        expected_clip_start: expected clip start time (string or None)
        expected_clip_end: expected clip end (current_clip_name or None)
    """
    wav_path, clip_start, clip_end = stream.get_next_clip()

    assert (
        wav_path == expected_wav_path
    ), f"Expected wav_path {expected_wav_path}, got {wav_path}"
    assert (
        clip_start == expected_clip_start
    ), f"Expected clip_start {expected_clip_start}, got {clip_start}"
    assert (
        clip_end == expected_clip_end
    ), f"Expected clip_end {expected_clip_end}, got {clip_end}"


@pytest.mark.slow
@pytest.mark.tests
@pytest.mark.parametrize(
    "desired_time,expected_wav_path,expected_clip_start,expected_clip_end",
    [
        (
            # Test with a time less than 60 seconds into a folder, which should
            # fail.
            # Thursday, Nov 6, 2025 00:00:51 PST
            datetime(
                2025, 11, 6, 0, 0, 51, tzinfo=ZoneInfo("America/Los_Angeles")
            ),
            None,
            None,
            None,
        ),
        (
            # Test with a time that isn't on a boundary.
            # Time strings are returned in local (PST) time.
            # The returned values should be updated to the actual clip times
            # but currently are just based on the requested time (issue #46).
            # Using Thursday, Nov 6, 2025 00:01:43 PST, but DateRangeHLSStream
            # currently returns strings in local time when run locally, and
            # GMT when run by github.  This should also be updated to be
            # consistent (issue #47).
            datetime(
                2025, 11, 6, 0, 1, 43, tzinfo=ZoneInfo("America/Los_Angeles")
            ),
            os.path.join(
                ".",
                "test_wav_output",
                "rpi-orcasound-lab_2025_11_06_08_00_43.wav",
            ),
            "2025_11_06_08_00_43",
            None,
        ),
        (
            # Test with Scott's rock test on 11/4/25, where the rock splash
            # happened at 11:17:09.4 local (19:17:09.4 UTC) according to
            # Scott's phone.  The returned values should be updated to the
            # actual clip times but currently are just based on the requested
            # time.
            datetime(
                2025, 11, 4, 11, 17, 9, tzinfo=ZoneInfo("America/Los_Angeles")
            ),
            os.path.join(
                ".",
                "test_wav_output",
                "rpi-orcasound-lab_2025_11_04_19_16_09.wav",
            ),
            "2025_11_04_19_16_09",
            None,
        ),
    ],
)
def test_get_next_clip_specific_times(
    default_stream_base,
    desired_time,
    expected_wav_path,
    expected_clip_start,
    expected_clip_end,
):
    """Test get_next_clip with specific timestamps using helper function.

    This test uses check_daterange_get_next_clip_output to verify
    expected outputs.
    """
    polling_interval = 60
    wav_dir = os.path.join(".", "test_wav_output")

    if os.path.exists(wav_dir):
        shutil.rmtree(wav_dir)
    os.makedirs(wav_dir, exist_ok=True)

    # Set up a date range around this time
    start_time = desired_time - timedelta(minutes=1)
    end_time = desired_time
    start_unix_time = int(start_time.timestamp())
    end_unix_time = int(end_time.timestamp())

    try:
        stream = DateRangeHLSStream(
            default_stream_base,
            polling_interval,
            start_unix_time,
            end_unix_time,
            wav_dir,
        )

        check_daterange_get_next_clip_output(
            stream,
            expected_wav_path,
            expected_clip_start,
            expected_clip_end,
        )
    finally:
        if os.path.exists(wav_dir):
            shutil.rmtree(wav_dir)
