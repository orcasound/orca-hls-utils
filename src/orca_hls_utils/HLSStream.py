# HLSStream class
import math
import os
import shutil
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import ffmpeg  # ffmpeg-python
import m3u8
from botocore import UNSIGNED
from botocore.config import Config
from pytz import timezone

from . import datetime_utils, playlist_utils, scraper


def get_readable_clipname(hydrophone_id, cliptime_utc):
    # cliptime is of the form 2020-09-27T00/16/55.677242Z
    cliptime_utc = timezone("UTC").localize(cliptime_utc)
    date = cliptime_utc.astimezone(timezone("US/Pacific"))
    date_format = "%Y_%m_%d_%H_%M_%S_%Z"
    clipname = date.strftime(date_format)
    return hydrophone_id + "_" + clipname, date


# TODO (@prgogia) Handle errors due to rebooting of hydrophone
class HLSStream:
    """
    stream_base = 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab' # noqa
    polling_interval = 60 sec
    """

    def __init__(self, stream_base, polling_interval, wav_dir, audio_offset=2):
        self.stream_base = stream_base
        self.polling_interval = polling_interval
        self.wav_dir = wav_dir
        self.audio_offset = audio_offset
        bucket_folder = self.stream_base.split(
            "https://s3-us-west-2.amazonaws.com/"
        )[1]
        tokens = bucket_folder.split("/")
        self.s3_bucket = tokens[0]
        self.hydrophone_id = tokens[1]

    def get_latest_folder_time(self):
        latest = f"{self.stream_base}/latest.txt"
        try:
            with urllib.request.urlopen(latest) as response:
                stream_id = response.read().decode("utf-8").strip()
        except urllib.error.HTTPError as e:
            print(f"Failed to fetch latest.txt: {e}")
            return None
        except urllib.error.URLError as e:
            print(f"Failed to fetch latest.txt: {e}")
            return None
        return stream_id

    # this function grabs audio from last_end_time to
    def get_next_clip(self, current_clip_end_time):
        # if current time < current_clip_end_time, sleep for the difference
        now = datetime.utcnow()

        # the extra 10 seconds to sleep is to download the last .ts segment
        # properly
        time_to_sleep = (current_clip_end_time - now).total_seconds() + 10

        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

        # get latest AWS bucket
        print("Listening to location {loc}".format(loc=self.stream_base))
        stream_id = self.get_latest_folder_time()
        if stream_id is None:
            return None, None, current_clip_end_time

        # stream_url for the current AWS bucket
        stream_url = "{}/hls/{}/live.m3u8".format(
            (self.stream_base), (stream_id)
        )

        # check if m3u8 file exists
        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        prefix = "{}/hls/{}/live.m3u8".format(self.hydrophone_id, stream_id)
        objs = s3.list_objects_v2(
            Bucket=self.s3_bucket, Prefix=prefix, MaxKeys=1, Delimiter="/"
        )

        # if it does not, exit
        if objs["KeyCount"] == 0:
            print(".m3u8 file does not exist, will retry after some time")
            return None, None, current_clip_end_time

        assert objs["KeyCount"] == 1

        # .m3u8 file exists so load it
        stream_obj = m3u8.load(stream_url)
        num_total_segments = len(stream_obj.segments)
        sequence_start = getattr(stream_obj, "media_sequence", 0) or 0
        use_timeline = sequence_start > 0 or (
            playlist_utils.has_program_date_time(stream_obj)
        )

        # Prefer timeline-based clipping when sequence/PDT metadata exists,
        # so clip boundaries follow real segment timing instead of averages.
        if use_timeline:
            timeline = playlist_utils.get_playlist_timeline(
                stream_obj, int(stream_id), self.audio_offset
            )
            # Convert the requested clip window to Unix seconds for timeline matching.
            request_end = playlist_utils.unix_time_from_utc_datetime(
                current_clip_end_time
            )
            request_start = request_end - self.polling_interval
            earliest_live = timeline[0]

            # If the requested start is older than current live coverage, try
            # extending coverage with archive playlist data.
            if request_start < earliest_live.start:
                try:
                    archive = m3u8.load(
                        "{}/hls/{}/archive.m3u8".format(
                            self.stream_base, stream_id
                        )
                    )
                except Exception:
                    archive = None

                archive_timeline = []
                if archive is not None and archive.segments:
                    archive_timeline = playlist_utils.get_playlist_timeline(
                        archive, int(stream_id), self.audio_offset
                    )
                    # Merge archive and live entries by sequence number.
                    timeline = playlist_utils.merge_timelines(
                        archive_timeline, timeline
                    )

                # Fill small gaps between archive and live timelines when they
                # are adjacent but not directly contiguous.
                if archive_timeline and (
                    archive_timeline[-1].sequence < earliest_live.sequence - 1
                ):
                    timeline = playlist_utils.merge_timelines(
                        timeline,
                        playlist_utils.extrapolate_before(
                            [earliest_live],
                            archive_timeline[-1].start
                            + archive_timeline[-1].duration,
                            archive_timeline[-1].sequence,
                        ),
                    )
                # If no archive exists and request is only slightly before the
                # first live segment, estimate prior segments heuristically.
                elif not archive_timeline and (
                    request_start < earliest_live.start
                    and earliest_live.start - request_start <= 180
                ):
                    timeline = playlist_utils.merge_timelines(
                        playlist_utils.extrapolate_before(
                            timeline, request_start
                        ),
                        timeline,
                    )

            # Choose a contiguous segment set that ends at/after request_end and
            # covers at least one polling interval.
            selected_segments = playlist_utils.select_segments_ending_at(
                timeline, request_end, self.polling_interval
            )
            if not selected_segments:
                return None, None, current_clip_end_time

            # Use actual segment times for output metadata and scheduler state.
            actual_start = selected_segments[0].start
            actual_end = (
                selected_segments[-1].start + selected_segments[-1].duration
            )
            current_clip_end_time = datetime.utcfromtimestamp(actual_end)
            current_clip_start_time = datetime.utcfromtimestamp(actual_start)

            # Download and process selected segments in isolated temp storage.
            with tempfile.TemporaryDirectory() as tmp_path:
                file_names = []
                for item in selected_segments:
                    file_name = item.url.rsplit("/", 1)[-1]
                    try:
                        scraper.download_from_url(item.url, tmp_path)
                        file_names.append(file_name)
                    except Exception:
                        print("Skipping", item.url, ": error.")

                clip_start_time = current_clip_start_time.isoformat() + "Z"
                clipname, _ = get_readable_clipname(
                    self.hydrophone_id, current_clip_start_time
                )
                hls_file = clipname + ".ts"
                audio_file = clipname + ".wav"
                wav_file_path = os.path.join(self.wav_dir, audio_file)
                hls_file_path = os.path.join(tmp_path, hls_file)

                # Concatenate segments before converting to the destination WAV.
                with open(hls_file_path, "wb") as outfile:
                    for fname in file_names:
                        with open(
                            os.path.join(tmp_path, fname), "rb"
                        ) as infile:
                            outfile.write(infile.read())

                try:
                    stream = ffmpeg.input(
                        os.path.join(tmp_path, Path(hls_file))
                    )
                    stream = ffmpeg.output(stream, wav_file_path)
                    ffmpeg.run(stream, quiet=True)
                except ffmpeg.Error as e:
                    print("FFmpeg command failed.")
                    print("stdout:", e.stdout.decode("utf8", errors="ignore"))
                    print("stderr:", e.stderr.decode("utf8", errors="ignore"))
                    raise

            return wav_file_path, clip_start_time, current_clip_end_time

        target_duration = round(
            sum([item.duration for item in stream_obj.segments])
            / num_total_segments,
            3,
        )
        num_segments_in_wav_duration = math.ceil(
            self.polling_interval / target_duration
        )

        # calculate the start index by computing the current time - start of
        # current folder
        current_clip_end_time_unix_pst = (
            datetime_utils.get_unix_time_from_datetime_utc(
                current_clip_end_time
            )
        )
        time_since_folder_start = (
            datetime_utils.get_difference_between_times_in_seconds(
                current_clip_end_time_unix_pst, stream_id
            )
        )

        # Currently there is a delay between the stream_id time and
        # the actual start of the audio stream in the folder, so add
        # an offset here to compensate.
        time_since_folder_start -= self.audio_offset

        if time_since_folder_start < self.polling_interval + 20:
            # This implies that possibly a new folder was created
            # and we do not have enough data for a 1 minute clip + 20 second
            # buffer
            # we exit and try again after hls polling interval
            print("not enough data for a 1 minute clip + 20 second buffer")
            return None, None, current_clip_end_time

        min_num_total_segments_required = math.ceil(
            time_since_folder_start / target_duration
        )
        segment_start_index = (
            min_num_total_segments_required - num_segments_in_wav_duration
        )
        segment_end_index = segment_start_index + num_segments_in_wav_duration

        # Compute nominal end time
        end_seconds = (
            segment_end_index * target_duration
            + int(stream_id)
            + self.audio_offset
        )
        end_utc = datetime.utcfromtimestamp(end_seconds)
        current_clip_end_time = end_utc

        if segment_end_index > num_total_segments:
            return None, None, current_clip_end_time

        # Create tmp path to hold .ts segments
        tmp_path = "tmp_path"
        os.makedirs(tmp_path, exist_ok=True)

        file_names = []
        for i in range(segment_start_index, segment_end_index):
            audio_segment = stream_obj.segments[i]
            file_name = playlist_utils.segment_filename(audio_segment)
            audio_url = playlist_utils.resolve_segment_url(audio_segment)
            try:
                scraper.download_from_url(audio_url, tmp_path)
                file_names.append(file_name)
            except Exception:
                print("Skipping", audio_url, ": error.")

        current_clip_start_time = current_clip_end_time - timedelta(0, 60)
        clip_start_time = current_clip_start_time.isoformat() + "Z"
        clipname, _ = get_readable_clipname(
            self.hydrophone_id, current_clip_start_time
        )

        # concatentate all .ts files with ffmpeg
        hls_file = clipname + ".ts"
        audio_file = clipname + ".wav"
        wav_file_path = os.path.join(self.wav_dir, audio_file)
        hls_file_path = os.path.join(tmp_path, hls_file)

        # Combine .ts files into one .ts file
        with open(hls_file_path, "wb") as outfile:
            for fname in file_names:
                segment_path = os.path.join(tmp_path, fname)
                with open(segment_path, "rb") as infile:
                    outfile.write(infile.read())

        # read the concatenated .ts and write to wav
        try:
            stream = ffmpeg.input(os.path.join(tmp_path, Path(hls_file)))
            stream = ffmpeg.output(stream, wav_file_path)
            ffmpeg.run(stream, quiet=True)
        except ffmpeg.Error as e:
            print("FFmpeg command failed.")
            print("stdout:", e.stdout.decode("utf8", errors="ignore"))
            print("stderr:", e.stderr.decode("utf8", errors="ignore"))
            raise

        # clear the tmp_path
        shutil.rmtree(tmp_path, ignore_errors=True)

        return wav_file_path, clip_start_time, current_clip_end_time

    def is_stream_over(self):
        # returns true or false based on whether the stream is over
        # for a live stream, the stream is never over
        return False
