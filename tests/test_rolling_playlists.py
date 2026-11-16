import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import m3u8
import pytest

from orca_hls_utils import playlist_utils
from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream
from orca_hls_utils.HLSStream import HLSStream

FOLDER = 1700000000
STREAM_BASE = (
    "https://s3-us-west-2.amazonaws.com/audio-orcasound-net/"
    "rpi_orcasound_lab"
)


def _playlist(
    first,
    count,
    uri="live{:03d}.ts",
    audio_offset=2,
    program_date_time_offset=0,
):
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:{}".format(first),
    ]
    for sequence in range(first, first + count):
        start = datetime.fromtimestamp(
            FOLDER
            + audio_offset
            + sequence * 10
            + program_date_time_offset,
            tz=timezone.utc,
        )
        lines.extend(
            [
                "#EXT-X-PROGRAM-DATE-TIME:"
                + start.isoformat(timespec="milliseconds"),
                "#EXTINF:10.0,",
                uri.format(sequence),
            ]
        )
    return "\n".join(lines)


def _mock_media(monkeypatch, tmp_path, module, playlist_loader):
    module = importlib.import_module(module.__module__)
    monkeypatch.setattr(
        module.scraper,
        "download_from_url",
        lambda url, dest: Path(dest, url.rsplit("/", 1)[-1]).write_bytes(
            b"ts"
        ),
    )
    monkeypatch.setattr(module.ffmpeg, "input", lambda *args, **kwargs: "in")
    monkeypatch.setattr(
        module.ffmpeg, "output", lambda stream, path, **kwargs: path
    )
    monkeypatch.setattr(
        module.ffmpeg,
        "run",
        lambda path, **kwargs: Path(path).write_bytes(b"RIFF"),
    )
    monkeypatch.setattr(module.m3u8, "load", playlist_loader)


@pytest.mark.tests
def test_daterange_uses_archive_and_program_date_time(monkeypatch, tmp_path):
    loaded_urls = []

    def load(url):
        loaded_urls.append(url)
        if url.endswith("/archive.m3u8"):
            return m3u8.loads(_playlist(0, 30), uri=url)
        return m3u8.loads(_playlist(20, 10), uri=url)

    _mock_media(monkeypatch, tmp_path, DateRangeHLSStream, load)
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(FOLDER)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils."
        "get_folders_between_timestamp",
        lambda *args, **kwargs: [FOLDER],
    )

    request_start = FOLDER + 2 + 5 * 10
    stream = DateRangeHLSStream(
        STREAM_BASE,
        60,
        request_start,
        request_start + 60,
        str(tmp_path),
    )
    wav_path, clip_start, _ = stream.get_next_clip()

    assert loaded_urls[0].endswith("/archive.m3u8")
    assert clip_start == datetime.fromtimestamp(
        request_start, tz=timezone.utc
    ).astimezone().strftime("%Y_%m_%d_%H_%M_%S")
    assert wav_path.endswith("rpi-orcasound-lab_{}.wav".format(clip_start))


@pytest.mark.tests
def test_hls_rolling_playlist_uses_sequence_urls_before_live_window(
    monkeypatch, tmp_path
):
    live = m3u8.loads(
        _playlist(3876, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    downloaded = []

    def load(url):
        if url.endswith("/live.m3u8"):
            return live
        raise ValueError("archive does not exist")

    _mock_media(monkeypatch, tmp_path, HLSStream, load)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.boto3.client",
        lambda *args, **kwargs: type(
            "S3", (), {"list_objects_v2": lambda *a, **k: {"KeyCount": 1}}
        )(),
    )
    monkeypatch.setattr(
        HLSStream, "get_latest_folder_time", lambda self: str(FOLDER)
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.time.sleep", lambda *args: None
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )

    request_end = datetime.fromtimestamp(
        FOLDER + 2 + (3878 + 1) * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))
    wav_path, clip_start, clip_end = stream.get_next_clip(request_end)

    assert [url.rsplit("/", 1)[-1] for url in downloaded] == [
        "live3873.ts",
        "live3874.ts",
        "live3875.ts",
        "live3876.ts",
        "live3877.ts",
        "live3878.ts",
    ]
    assert (
        clip_start
        == datetime.fromtimestamp(FOLDER + 2 + 3873 * 10, tz=timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
        + "Z"
    )
    assert clip_end == datetime.fromtimestamp(
        FOLDER + 2 + 3879 * 10, tz=timezone.utc
    ).replace(tzinfo=None)
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_hls_older_window_uses_archive_playlist(monkeypatch, tmp_path):
    loaded_urls = []
    downloaded = []
    live = m3u8.loads(
        _playlist(60, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    archive = m3u8.loads(
        _playlist(0, 60),
        uri="{}/hls/{}/archive.m3u8".format(STREAM_BASE, FOLDER),
    )

    def load(url):
        loaded_urls.append(url)
        if url.endswith("/live.m3u8"):
            return live
        if url.endswith("/archive.m3u8"):
            return archive
        raise ValueError("unexpected playlist")

    _mock_media(monkeypatch, tmp_path, HLSStream, load)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.boto3.client",
        lambda *args, **kwargs: type(
            "S3", (), {"list_objects_v2": lambda *a, **k: {"KeyCount": 1}}
        )(),
    )
    monkeypatch.setattr(
        HLSStream, "get_latest_folder_time", lambda self: str(FOLDER)
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.time.sleep", lambda *args: None
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )

    request_end = datetime.fromtimestamp(
        FOLDER + 2 + (50 + 1) * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))
    wav_path, _, _ = stream.get_next_clip(request_end)

    assert any(url.endswith("/archive.m3u8") for url in loaded_urls)
    assert [url.rsplit("/", 1)[-1] for url in downloaded] == [
        "live045.ts",
        "live046.ts",
        "live047.ts",
        "live048.ts",
        "live049.ts",
        "live050.ts",
    ]
    assert Path(wav_path).exists()


@pytest.mark.tests
@pytest.mark.parametrize(
    "desired_end_offset,audio_offset,expected_start_offset,expected_end_offset",
    [
        (31, 2, None, 31),
        (103, 2, 52.167, 112.167),
        (103, 1, 51.167, 111.167),
    ],
)
def test_hls_specific_clip_times(
    monkeypatch,
    tmp_path,
    desired_end_offset,
    audio_offset,
    expected_start_offset,
    expected_end_offset,
):
    playlist = m3u8.loads(
        _playlist(
            0,
            30,
            audio_offset=audio_offset,
            program_date_time_offset=0.167,
        ),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    _mock_media(monkeypatch, tmp_path, HLSStream, lambda url: playlist)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.boto3.client",
        lambda *args, **kwargs: type(
            "S3", (), {"list_objects_v2": lambda *a, **k: {"KeyCount": 1}}
        )(),
    )
    monkeypatch.setattr(
        HLSStream, "get_latest_folder_time", lambda self: str(FOLDER)
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.time.sleep", lambda *args: None
    )

    stream = HLSStream(STREAM_BASE, 60, str(tmp_path), audio_offset)
    request_end = datetime.fromtimestamp(
        FOLDER + desired_end_offset, tz=timezone.utc
    ).replace(tzinfo=None)
    _, clip_start, clip_end = stream.get_next_clip(request_end)

    expected_start = (
        datetime.fromtimestamp(
            FOLDER + expected_start_offset, tz=timezone.utc
        ).replace(tzinfo=None).isoformat()
        + "Z"
        if expected_start_offset is not None
        else None
    )
    expected_end = datetime.fromtimestamp(
        FOLDER + expected_end_offset, tz=timezone.utc
    ).replace(tzinfo=None)
    assert clip_start == expected_start
    assert clip_end == expected_end


@pytest.mark.tests
def test_sequence_extrapolation_keeps_minimum_padding_after_999():
    playlist = m3u8.loads(
        _playlist(1000, 2),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    timeline = playlist_utils.get_playlist_timeline(
        playlist, FOLDER, audio_offset=2
    )

    earlier = playlist_utils.extrapolate_before(
        timeline, timeline[0].start - 20
    )

    assert [item.url.rsplit("/", 1)[-1] for item in earlier] == [
        "live998.ts",
        "live999.ts",
    ]


@pytest.mark.tests
def test_timeline_preserves_absolute_segment_uris():
    playlist = m3u8.loads(
        _playlist(1000, 2, uri="https://cdn.example/audio/live{}.ts"),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

    timeline = playlist_utils.get_playlist_timeline(
        playlist, FOLDER, audio_offset=2
    )
    earlier = playlist_utils.extrapolate_before(
        timeline, timeline[0].start - 20
    )

    assert timeline[0].url == "https://cdn.example/audio/live1000.ts"
    assert [item.url for item in earlier] == [
        "https://cdn.example/audio/live998.ts",
        "https://cdn.example/audio/live999.ts",
    ]


@pytest.mark.tests
def test_sequential_clips_do_not_overlap_when_program_dates_are_rounded(
    monkeypatch, tmp_path
):
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:11",
        "#EXT-X-MEDIA-SEQUENCE:0",
    ]
    for sequence in range(20):
        start = datetime.fromtimestamp(
            FOLDER + 2 + sequence * 10, tz=timezone.utc
        )
        lines.extend(
            [
                "#EXT-X-PROGRAM-DATE-TIME:"
                + start.isoformat(timespec="milliseconds"),
                "#EXTINF:10.000289,",
                "live{:03d}.ts".format(sequence),
            ]
        )
    playlist = m3u8.loads(
        "\n".join(lines),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

    _mock_media(monkeypatch, tmp_path, HLSStream, lambda url: playlist)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.boto3.client",
        lambda *args, **kwargs: type(
            "S3", (), {"list_objects_v2": lambda *a, **k: {"KeyCount": 1}}
        )(),
    )
    monkeypatch.setattr(
        HLSStream, "get_latest_folder_time", lambda self: str(FOLDER)
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.time.sleep", lambda *args: None
    )

    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))
    first_end = datetime.fromtimestamp(
        FOLDER + 2 + 7 * 10, tz=timezone.utc
    ).replace(tzinfo=None)
    _, _, clip_end = stream.get_next_clip(first_end)
    _, next_clip_start, _ = stream.get_next_clip(
        clip_end + timedelta(seconds=60)
    )

    assert datetime.fromisoformat(next_clip_start.rstrip("Z")) >= clip_end


@pytest.mark.tests
def test_archive_loader_falls_back_to_live(monkeypatch):
    requested = []

    def load(url):
        requested.append(url)
        if url.endswith("/archive.m3u8"):
            raise ValueError("missing")
        return m3u8.loads(_playlist(0, 2), uri=url)

    monkeypatch.setattr("orca_hls_utils.playlist_utils.m3u8.load", load)
    from orca_hls_utils.playlist_utils import load_archive_or_live

    playlist = load_archive_or_live(STREAM_BASE, FOLDER)

    assert len(playlist.segments) == 2
    assert requested[0].endswith("/archive.m3u8")
    assert requested[1].endswith("/live.m3u8")
