import math
import re
from collections import namedtuple
from datetime import timezone
from urllib.parse import urljoin, urlsplit, urlunsplit

import m3u8

PlaylistSegment = namedtuple(
    "PlaylistSegment", ["segment", "sequence", "start", "duration", "url"]
)


def load_archive_or_live(stream_base, folder):
    """Load a full archive playlist when available, otherwise the live one."""
    for name in ("archive.m3u8", "live.m3u8"):
        url = "{}/hls/{}/{}".format(stream_base, folder, name)
        try:
            playlist = m3u8.load(url)
            if playlist.segments:
                return playlist
        except Exception:
            if name == "live.m3u8":
                raise


def resolve_segment_url(segment):
    """Resolve a segment URI against its playlist base URI."""
    return urljoin(segment.base_uri or "", segment.uri)


def segment_filename(segment):
    """Return the local filename for a segment URI."""
    return resolve_segment_url(segment).rsplit("/", 1)[-1]


def get_playlist_timeline(playlist, folder_epoch, audio_offset):
    """Return playlist segments with sequence numbers and estimated starts."""
    segments = playlist.segments
    if not segments:
        return []

    durations = [float(segment.duration) for segment in segments]
    sequence_start = getattr(playlist, "media_sequence", 0) or 0
    dates = [
        getattr(segment, "current_program_date_time", None)
        or getattr(segment, "program_date_time", None)
        for segment in segments
    ]
    first_date = next(
        (index for index, value in enumerate(dates) if value is not None), None
    )

    if first_date is not None:
        starts = [None] * len(segments)
        starts[first_date] = dates[first_date].timestamp()
        for index in range(first_date - 1, -1, -1):
            starts[index] = starts[index + 1] - durations[index]
        for index in range(first_date + 1, len(segments)):
            previous_end = starts[index - 1] + durations[index - 1]
            program_date_time = (
                dates[index].timestamp()
                if dates[index] is not None
                else previous_end
            )
            starts[index] = max(program_date_time, previous_end)
    else:
        mean_duration = sum(durations) / len(durations)
        starts = []
        current_start = folder_epoch + audio_offset
        for index in range(len(segments)):
            if sequence_start:
                current_start = (
                    folder_epoch
                    + audio_offset
                    + (sequence_start + index) * mean_duration
                )
            starts.append(current_start)
            current_start += durations[index]

    return [
        PlaylistSegment(
            segment,
            sequence_start + index,
            starts[index],
            durations[index],
            resolve_segment_url(segment),
        )
        for index, segment in enumerate(segments)
    ]


def has_program_date_time(playlist):
    return any(
        getattr(segment, "current_program_date_time", None)
        or getattr(segment, "program_date_time", None)
        for segment in playlist.segments
    )


def extrapolate_before(timeline, start_time, min_sequence=-1):
    """Estimate earlier rolling-window segments from the first PDT."""
    if not timeline or timeline[0].sequence <= 0:
        return []

    first = timeline[0]
    delta = first.start - start_time
    if delta <= 0:
        return []
    mean_duration = sum(item.duration for item in timeline) / len(timeline)
    parsed_url = urlsplit(first.url)
    segment_name = parsed_url.path.rsplit("/", 1)[-1]
    match = re.match(r"^(.*?)(\d+)(\.ts)$", segment_name)
    if not match or mean_duration <= 0:
        return []

    width = len(match.group(2)) if match.group(2).startswith("0") else 3
    base_uri = urlunsplit(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path.rsplit("/", 1)[0] + "/",
            "",
            "",
        )
    )
    count = min(first.sequence, math.ceil(delta / mean_duration - 1e-9))
    earlier = []
    for distance in range(count, 0, -1):
        sequence = first.sequence - distance
        if sequence <= min_sequence:
            continue
        uri = "{}{:0{}d}{}".format(
            match.group(1), sequence, width, match.group(3)
        )
        earlier.append(
            PlaylistSegment(
                None,
                sequence,
                first.start - distance * mean_duration,
                mean_duration,
                base_uri + uri,
            )
        )
    return earlier


def merge_timelines(*timelines):
    """Merge timelines by sequence number, preferring later (newer) entries."""
    segments = {}
    for timeline in timelines:
        for item in timeline:
            segments[item.sequence] = item
    return [segments[index] for index in sorted(segments)]


def select_segments_from_start(timeline, start_time, duration):
    """Select a contiguous clip beginning at the next segment start."""
    if not timeline or timeline[0].start > start_time + 1e-6:
        return []
    start_index = next(
        (
            index
            for index, item in enumerate(timeline)
            if item.start >= start_time - 1e-6
        ),
        None,
    )
    if start_index is None:
        return []

    selected = []
    elapsed = 0.0
    for item in timeline[start_index:]:
        if selected and item.sequence != selected[-1].sequence + 1:
            break
        selected.append(item)
        elapsed += item.duration
        if elapsed >= duration:
            return selected
    return []


def select_segments_ending_at(timeline, end_time, duration):
    """Select a contiguous clip ending at the next segment boundary."""
    if not timeline or timeline[0].start > end_time + 1e-6:
        return []
    end_index = next(
        (
            index
            for index, item in enumerate(timeline)
            if item.start + item.duration >= end_time - 1e-6
        ),
        None,
    )
    if end_index is None:
        return []

    selected = []
    elapsed = 0.0
    for item in reversed(timeline[: end_index + 1]):
        if selected and item.sequence + 1 != selected[0].sequence:
            break
        selected.insert(0, item)
        elapsed += item.duration
        if elapsed >= duration:
            return selected
    return []


def unix_time_from_utc_datetime(value):
    return value.replace(tzinfo=timezone.utc).timestamp()
