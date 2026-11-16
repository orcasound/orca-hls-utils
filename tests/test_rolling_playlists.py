import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import m3u8
import pytest

from orca_hls_utils import playlist_utils, s3_utils
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
    program_date_time=True,
    folder_epoch=FOLDER,
):
    """Build an in-memory HLS playlist string for test scenarios."""
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:10",
        "#EXT-X-MEDIA-SEQUENCE:{}".format(first),
    ]
    for sequence in range(first, first + count):
        if program_date_time:
            start = datetime.fromtimestamp(
                folder_epoch
                + audio_offset
                + sequence * 10
                + program_date_time_offset,
                tz=timezone.utc,
            )
            lines.append(
                "#EXT-X-PROGRAM-DATE-TIME:"
                + start.isoformat(timespec="milliseconds")
            )
        lines.extend(["#EXTINF:10.0,", uri.format(sequence)])
    return "\n".join(lines)


def _mock_media(monkeypatch, tmp_path, module, playlist_loader):
    """Mock downloader, ffmpeg, and playlist loader dependencies."""
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
    monkeypatch.setattr(
        importlib.import_module("orca_hls_utils.playlist_utils").m3u8,
        "load",
        playlist_loader,
    )


@pytest.mark.tests
def test_folder_range_includes_previous_session_and_handles_empty_list():
    """Folder selection includes sessions that can extend into the range."""
    folders = [str(FOLDER - 2 * 86400), str(FOLDER - 86400), str(FOLDER)]

    assert s3_utils.get_folders_between_timestamp(
        folders, str(FOLDER - 60), str(FOLDER + 60)
    ) == [FOLDER - 86400, FOLDER]
    assert s3_utils.get_folders_between_timestamp([], "1", "2") == []


@pytest.mark.tests
def test_daterange_uses_archive_and_program_date_time(monkeypatch, tmp_path):
    """DateRange stream uses archive data and aligns clips using PDT."""
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
def test_daterange_nonzero_media_sequence_uses_timeline(monkeypatch, tmp_path):
    """DateRange stream uses timeline selection when media sequence is
    nonzero."""
    downloaded = []
    playlist = m3u8.loads(
        _playlist(20, 10, program_date_time=False),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

    def load(url):
        if url.endswith("/archive.m3u8"):
            raise ValueError("archive does not exist")
        return playlist

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
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )

    request_start = FOLDER + 2 + 20 * 10
    stream = DateRangeHLSStream(
        STREAM_BASE,
        60,
        request_start,
        request_start + 60,
        str(tmp_path),
    )
    wav_path, _, _ = stream.get_next_clip()

    assert [url.rsplit("/", 1)[-1] for url in downloaded] == [
        "live020.ts",
        "live021.ts",
        "live022.ts",
        "live023.ts",
        "live024.ts",
        "live025.ts",
    ]
    assert stream.last_clip_approximate
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_daterange_bridges_archive_lag_only_for_existing_segments(
    monkeypatch, tmp_path
):
    """DateRange fills archive gaps after checking inferred segment URLs."""
    downloaded = []
    checked = []
    archive = m3u8.loads(
        _playlist(0, 4),
        uri="{}/hls/{}/archive.m3u8".format(STREAM_BASE, FOLDER),
    )
    live = m3u8.loads(
        _playlist(6, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

    def load(url):
        return archive if url.endswith("/archive.m3u8") else live

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

    def check(url):
        checked.append(url)
        return url.endswith(("live004.ts", "live005.ts"))

    monkeypatch.setattr(playlist_utils, "url_exists", check)
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )
    stream = DateRangeHLSStream(
        STREAM_BASE,
        60,
        FOLDER + 2,
        FOLDER + 62,
        str(tmp_path),
    )

    wav_path, _, _ = stream.get_next_clip()

    assert [url.rsplit("/", 1)[-1] for url in checked] == [
        "live004.ts",
        "live005.ts",
    ]
    assert [url.rsplit("/", 1)[-1] for url in downloaded] == [
        "live000.ts",
        "live001.ts",
        "live002.ts",
        "live003.ts",
        "live004.ts",
        "live005.ts",
    ]
    assert stream.last_clip_approximate
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_daterange_limits_start_extrapolation_for_rolling_playlist(
    monkeypatch, tmp_path
):
    """A historical request does not probe every sequence before live data."""
    playlist = m3u8.loads(
        _playlist(100, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    checked = []
    _mock_media(
        monkeypatch, tmp_path, DateRangeHLSStream, lambda url: playlist
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(FOLDER)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils."
        "get_folders_between_timestamp",
        lambda *args, **kwargs: [FOLDER],
    )
    monkeypatch.setattr(
        playlist_utils, "url_exists", lambda url: checked.append(url) or True
    )

    start = FOLDER + 2
    stream = DateRangeHLSStream(
        STREAM_BASE, 60, start, start + 60, str(tmp_path)
    )

    assert stream.get_next_clip() == (None, None, None)
    assert checked == []


@pytest.mark.tests
def test_daterange_advances_past_later_run_after_sequence_gap(
    monkeypatch, tmp_path
):
    """A later contiguous run is not selected again after an unresolved gap."""
    archive = m3u8.loads(
        _playlist(0, 3),
        uri="{}/hls/{}/archive.m3u8".format(STREAM_BASE, FOLDER),
    )
    live = m3u8.loads(
        _playlist(9, 6),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    downloaded = []

    def load(url):
        return archive if url.endswith("/archive.m3u8") else live

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
    monkeypatch.setattr(playlist_utils, "url_exists", lambda url: False)
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )
    start = FOLDER + 32
    stream = DateRangeHLSStream(
        STREAM_BASE, 60, start, start + 120, str(tmp_path)
    )

    wav_path, _, _ = stream.get_next_clip()

    assert [url.rsplit("/", 1)[-1] for url in downloaded] == [
        "live009.ts",
        "live010.ts",
        "live011.ts",
        "live012.ts",
        "live013.ts",
        "live014.ts",
    ]
    assert stream.current_clip_start_time == FOLDER + 152
    assert stream.get_next_clip() == (None, None, None)
    assert len(downloaded) == 6
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_daterange_download_failure_keeps_clip_pending(monkeypatch, tmp_path):
    """A failed segment download leaves the requested clip pending."""
    playlist = m3u8.loads(
        _playlist(0, 20),
        uri="{}/hls/{}/archive.m3u8".format(STREAM_BASE, FOLDER),
    )
    _mock_media(
        monkeypatch, tmp_path, DateRangeHLSStream, lambda url: playlist
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(FOLDER)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils."
        "get_folders_between_timestamp",
        lambda *args, **kwargs: [FOLDER],
    )

    def fail_download(url, dest):
        raise OSError("segment unavailable")

    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        fail_download,
    )
    start = FOLDER + 2
    stream = DateRangeHLSStream(
        STREAM_BASE, 60, start, start + 60, str(tmp_path)
    )

    with pytest.raises(RuntimeError, match="Failed to download selected"):
        stream.get_next_clip()

    assert stream.current_clip_start_time == start
    assert not list(Path(tmp_path).glob("*.wav"))


@pytest.mark.tests
def test_daterange_rolls_to_folder_for_requested_timestamp(
    monkeypatch, tmp_path
):
    """DateRange chooses the folder containing the requested clip."""
    previous_folder = FOLDER - 86400
    playlist = m3u8.loads(
        _playlist(0, 10, folder_epoch=FOLDER),
        uri="{}/hls/{}/archive.m3u8".format(STREAM_BASE, FOLDER),
    )
    downloaded = []

    def load(url):
        return playlist

    _mock_media(monkeypatch, tmp_path, DateRangeHLSStream, load)
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(previous_folder), str(FOLDER)],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.s3_utils."
        "get_folders_between_timestamp",
        lambda *args, **kwargs: [previous_folder, FOLDER],
    )
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )
    stream = DateRangeHLSStream(
        STREAM_BASE, 60, FOLDER, FOLDER + 60, str(tmp_path)
    )

    result = stream.get_next_clip()

    assert result[0] is not None
    assert stream.current_folder_index == 1
    assert downloaded


@pytest.mark.tests
def test_daterange_legacy_path_resolves_absolute_segment_uris(
    monkeypatch, tmp_path
):
    """DateRange legacy path preserves absolute segment URLs."""
    downloaded = []
    playlist = m3u8.loads(
        _playlist(
            0,
            20,
            uri="https://cdn.example/audio/live{:03d}.ts",
            program_date_time=False,
        ),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

    def load(url):
        if url.endswith("/archive.m3u8"):
            raise ValueError("archive does not exist")
        return playlist

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
    monkeypatch.setattr(
        "orca_hls_utils.DateRangeHLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )

    request_start = FOLDER + 2 + 5 * 10
    stream = DateRangeHLSStream(
        STREAM_BASE,
        60,
        request_start,
        request_start + 60,
        str(tmp_path),
    )
    wav_path, _, _ = stream.get_next_clip()

    assert downloaded == [
        "https://cdn.example/audio/live{:03d}.ts".format(sequence)
        for sequence in range(5, 11)
    ]
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_hls_rolling_playlist_uses_sequence_urls_before_live_window(
    monkeypatch, tmp_path
):
    """Rolling HLS backfills by sequence when request starts before live
    window."""
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
    checked = []
    monkeypatch.setattr(
        playlist_utils,
        "url_exists",
        lambda url: (checked.append(url), True)[1],
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
    assert [url.rsplit("/", 1)[-1] for url in checked] == [
        "live3872.ts",
        "live3873.ts",
        "live3874.ts",
        "live3875.ts",
    ]
    assert stream.last_clip_approximate
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
@pytest.mark.parametrize("latest_exists", [True, False])
def test_hls_uses_previous_folder_at_session_rollover(
    monkeypatch, tmp_path, latest_exists
):
    """Use the prior folder when latest has no coverage."""
    previous_folder = FOLDER - 86400
    latest = m3u8.loads(
        _playlist(0, 10, folder_epoch=FOLDER),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    previous = m3u8.loads(
        _playlist(40, 10, folder_epoch=previous_folder),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, previous_folder),
    )
    downloaded = []

    def load(url):
        return previous if str(previous_folder) in url else latest

    _mock_media(monkeypatch, tmp_path, HLSStream, load)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.boto3.client",
        lambda *args, **kwargs: type(
            "S3",
            (),
            {
                "list_objects_v2": lambda *a, **k: {
                    "KeyCount": int(latest_exists)
                }
            },
        )(),
    )
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.s3_utils.get_all_folders",
        lambda *args, **kwargs: [str(previous_folder), str(FOLDER)],
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
        previous_folder + 2 + 48 * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))

    wav_path, _, clip_end = stream.get_next_clip(request_end)

    assert downloaded
    assert all(str(previous_folder) in url for url in downloaded)
    assert clip_end > request_end
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_hls_retries_after_inferred_segment_is_missing(monkeypatch, tmp_path):
    """A missing inferred segment cannot create an incomplete clip."""
    live = m3u8.loads(
        _playlist(3876, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

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
        playlist_utils,
        "url_exists",
        lambda url: not url.endswith("live3874.ts"),
    )
    request_end = datetime.fromtimestamp(
        FOLDER + 2 + (3878 + 1) * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))

    result = stream.get_next_clip(request_end)

    assert result == (None, None, request_end)
    assert not stream.last_clip_approximate


@pytest.mark.tests
def test_start_selector_does_not_join_segments_across_a_gap():
    """A full clip can start after a gap but must not cross it."""
    timeline = [
        playlist_utils.PlaylistSegment(None, 0, 0, 10, "live000.ts"),
        playlist_utils.PlaylistSegment(None, 2, 20, 10, "live002.ts"),
        playlist_utils.PlaylistSegment(None, 3, 30, 10, "live003.ts"),
    ]

    selected = playlist_utils.select_segments_from_start(timeline, 0, 20)

    assert [item.sequence for item in selected] == [2, 3]


@pytest.mark.tests
def test_hls_download_failure_does_not_return_partial_clip(
    monkeypatch, tmp_path
):
    """A failed selected segment leaves the requested clip pending."""
    live = m3u8.loads(
        _playlist(3876, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )

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
    monkeypatch.setattr(playlist_utils, "url_exists", lambda url: True)
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.scraper.download_from_url",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            OSError("segment unavailable")
        ),
    )
    request_end = datetime.fromtimestamp(
        FOLDER + 2 + (3878 + 1) * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))

    result = stream.get_next_clip(request_end)

    assert result == (None, None, request_end)
    assert not stream.last_clip_approximate
    assert not list(Path(tmp_path).glob("*.wav"))


@pytest.mark.tests
def test_hls_rolling_playlist_cleans_temp_files_when_ffmpeg_fails(
    monkeypatch, tmp_path
):
    """Rolling playlist temp files are removed when transcoding fails."""
    live = m3u8.loads(
        _playlist(3876, 10),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    download_dirs = []

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
    monkeypatch.setattr(playlist_utils, "url_exists", lambda url: True)

    def download(url, dest):
        download_dirs.append(dest)
        Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts")

    def fail_ffmpeg(*args, **kwargs):
        raise RuntimeError("transcode failed")

    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.scraper.download_from_url", download
    )
    monkeypatch.setattr("orca_hls_utils.HLSStream.ffmpeg.run", fail_ffmpeg)

    request_end = datetime.fromtimestamp(
        FOLDER + 2 + (3878 + 1) * 10 - 1, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))

    with pytest.raises(RuntimeError, match="transcode failed"):
        stream.get_next_clip(request_end)

    assert download_dirs
    assert len(set(download_dirs)) == 1
    assert not Path(download_dirs[0]).exists()


@pytest.mark.tests
def test_hls_legacy_path_resolves_absolute_segment_uris(monkeypatch, tmp_path):
    """HLS legacy path preserves absolute segment URLs."""
    downloaded = []
    playlist = m3u8.loads(
        _playlist(
            0,
            20,
            uri="https://cdn.example/audio/live{:03d}.ts",
            program_date_time=False,
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
    monkeypatch.setattr(
        "orca_hls_utils.HLSStream.scraper.download_from_url",
        lambda url, dest: (
            downloaded.append(url),
            Path(dest, url.rsplit("/", 1)[-1]).write_bytes(b"ts"),
        ),
    )
    request_end = datetime.fromtimestamp(
        FOLDER + 2 + 80, tz=timezone.utc
    ).replace(tzinfo=None)
    stream = HLSStream(STREAM_BASE, 60, str(tmp_path))
    wav_path, _, _ = stream.get_next_clip(request_end)

    assert downloaded == [
        "https://cdn.example/audio/live{:03d}.ts".format(sequence)
        for sequence in range(2, 8)
    ]
    assert Path(wav_path).exists()


@pytest.mark.tests
def test_hls_older_window_uses_archive_playlist(monkeypatch, tmp_path):
    """HLS loads archive playlist when the requested clip predates live
    coverage."""
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
    "desired_end_offset,audio_offset,expected_start_offset,"
    "expected_end_offset",
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
    """HLS clip boundaries match expected timestamps across offset cases."""
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
        datetime.fromtimestamp(FOLDER + expected_start_offset, tz=timezone.utc)
        .replace(tzinfo=None)
        .isoformat()
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
    """Extrapolated sequence names keep minimum zero-padding after 999."""
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
def test_segment_selectors_reject_times_before_rolling_window():
    """Segment selectors return no clip before the rolling window starts."""
    playlist = m3u8.loads(
        _playlist(100, 4),
        uri="{}/hls/{}/live.m3u8".format(STREAM_BASE, FOLDER),
    )
    timeline = playlist_utils.get_playlist_timeline(
        playlist, FOLDER, audio_offset=2
    )
    stale_time = timeline[0].start - 1

    assert (
        playlist_utils.select_segments_from_start(timeline, stale_time, 20)
        == []
    )
    assert (
        playlist_utils.select_segments_ending_at(timeline, stale_time, 20)
        == []
    )


@pytest.mark.tests
def test_timeline_preserves_absolute_segment_uris():
    """Timeline and extrapolation preserve absolute segment URIs."""
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
    """Sequential clips do not overlap when PDT values are rounded."""
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
    """Archive loader falls back to live playlist when archive load fails."""
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
