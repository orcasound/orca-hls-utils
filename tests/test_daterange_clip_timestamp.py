#!/usr/bin/env python3
"""Offline tests for DateRangeHLSStream playlist-aligned clip timestamps.

These tests mock m3u8 playlists so they do not require live S3.  They
assert that get_next_clip returns the actual segment start (issue #46)
rather than a copy of the requested time.
"""

import math
import os
from datetime import datetime
from pathlib import Path

import pytest

from orca_hls_utils import datetime_utils
from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream


class _FakeSegment:
    def __init__(self, index, duration=10.0):
        self.duration = duration
        self.base_uri = "https://example.test/hls/folder/"
        self.uri = "live{:04d}.ts".format(index)


class _FakePlaylist:
    def __init__(self, num_segments=30, duration=10.0):
        self.segments = [
            _FakeSegment(i, duration) for i in range(num_segments)
        ]


def _expected_clip_start(
    folder_epoch, request_unix, target_duration, polling_interval, audio_offset
):
    """Mirror HLSStream / DateRangeHLSStream playlist start math."""
    time_since_folder_start = (
        datetime_utils.get_difference_between_times_in_seconds(
            request_unix, folder_epoch
        )
        - audio_offset
    )
    segment_start_index = math.ceil(time_since_folder_start / target_duration)
    num_segments_in_wav_duration = math.ceil(
        polling_interval / target_duration
    )
    segment_end_index = segment_start_index + num_segments_in_wav_duration
    end_seconds = (
        segment_end_index * target_duration + folder_epoch + audio_offset
    )
    return int(round(end_seconds - polling_interval))


def _install_offline_stream_fakes(
    monkeypatch, folder_epoch, num_segments=30, duration=10.0
):
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(folder_epoch)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils."
        "get_folders_between_timestamp",
        lambda folders, start, end: [int(folder_epoch)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.m3u8.load",
        lambda url: _FakePlaylist(num_segments, duration),
    )

    def fake_download(url, dest):
        Path(dest, url.rstrip("/").split("/")[-1]).write_bytes(b"ts")

    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        fake_download,
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.ffmpeg.input",
        lambda *args, **kwargs: "in",
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.ffmpeg.output",
        lambda stream, path, **kwargs: path,
    )

    def fake_run(stream, **kwargs):
        Path(stream).write_bytes(b"RIFF")

    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.ffmpeg.run", fake_run
    )


def _make_stream(
    monkeypatch,
    tmp_path,
    folder_epoch,
    request_unix,
    polling_interval=60,
    audio_offset=2,
    num_segments=30,
    duration=10.0,
):
    _install_offline_stream_fakes(
        monkeypatch, folder_epoch, num_segments, duration
    )
    stream_base = (
        "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
        "rpi_orcasound_lab"
    )
    return DateRangeHLSStream(
        stream_base,
        polling_interval,
        request_unix,
        request_unix + polling_interval,
        str(tmp_path),
        audio_offset=audio_offset,
    )


@pytest.mark.tests
@pytest.mark.parametrize(
    "offset_into_folder,audio_offset",
    [
        (43, 2),
        (43, 1),
        (17, 2),
    ],
)
def test_get_next_clip_returns_playlist_aligned_start(
    monkeypatch, tmp_path, offset_into_folder, audio_offset
):
    folder_epoch = 1700000000
    duration = 10.0
    polling_interval = 60
    request_unix = folder_epoch + offset_into_folder
    expected_start = _expected_clip_start(
        folder_epoch,
        request_unix,
        duration,
        polling_interval,
        audio_offset,
    )
    expected_name, expected_clip_start = (
        datetime_utils.get_clip_name_from_unix_time(
            "rpi-orcasound-lab", expected_start
        )
    )
    requested_name, requested_clip_start = (
        datetime_utils.get_clip_name_from_unix_time(
            "rpi-orcasound-lab", request_unix
        )
    )

    stream = _make_stream(
        monkeypatch,
        tmp_path,
        folder_epoch,
        request_unix,
        polling_interval=polling_interval,
        audio_offset=audio_offset,
    )
    wav_path, clip_start, clip_end = stream.get_next_clip()

    assert clip_start == expected_clip_start
    if expected_start != request_unix:
        assert clip_start != requested_clip_start
    returned_unix = int(
        datetime.strptime(clip_start, "%Y_%m_%d_%H_%M_%S").timestamp()
    )
    assert request_unix <= returned_unix <= request_unix + 60
    assert wav_path == os.path.join(str(tmp_path), expected_name + ".wav")
    assert clip_end is None


@pytest.mark.tests
def test_get_next_clip_aligned_request_matches_segment_start(
    monkeypatch, tmp_path
):
    """When the request already matches a segment start, keep that time."""
    folder_epoch = 1700000000
    audio_offset = 2
    duration = 10.0
    polling_interval = 60
    # folder + 52 is a playlist-aligned start for 10s segments + offset 2.
    request_unix = folder_epoch + 52
    expected_start = _expected_clip_start(
        folder_epoch,
        request_unix,
        duration,
        polling_interval,
        audio_offset,
    )
    assert expected_start == request_unix

    stream = _make_stream(
        monkeypatch,
        tmp_path,
        folder_epoch,
        request_unix,
        audio_offset=audio_offset,
    )
    _, clip_start, _ = stream.get_next_clip()
    _, expected_clip_start = datetime_utils.get_clip_name_from_unix_time(
        "rpi-orcasound-lab", expected_start
    )
    assert clip_start == expected_clip_start


@pytest.mark.tests
def test_get_next_clip_returns_none_when_playlist_too_short(
    monkeypatch, tmp_path
):
    folder_epoch = 1700000000
    request_unix = folder_epoch + 43
    stream = _make_stream(
        monkeypatch,
        tmp_path,
        folder_epoch,
        request_unix,
        num_segments=4,
    )
    wav_path, clip_start, clip_end = stream.get_next_clip()
    assert wav_path is None
    assert clip_start is None
    assert clip_end is None
