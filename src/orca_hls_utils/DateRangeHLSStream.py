import math
import os
import shutil

# import sys
import time
from datetime import datetime  # , timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import ffmpeg

# from botocore import UNSIGNED
# from botocore.config import Config
from pytz import timezone

from . import datetime_utils, playlist_utils, s3_utils, scraper


def get_readable_clipname(hydrophone_id, cliptime_utc):
    # cliptime is of the form 2020-09-27T00/16/55.677242Z
    cliptime_utc = timezone("UTC").localize(cliptime_utc)
    date = cliptime_utc.astimezone(timezone("US/Pacific"))
    date_format = "%Y_%m_%d_%H_%M_%S_%Z"
    clipname = date.strftime(date_format)
    return hydrophone_id + "_" + clipname, date


# TODO: Handle date ranges that don't exist
class DateRangeHLSStream:
    """
    stream_base = 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab' # noqa
    stream_base = 'https://s3-us-west-2.amazonaws.com/audio-orcasound-net/rpi_orcasound_lab' # noqa
    polling_interval = 60 sec
    start_unix_time
    end_unix_time
    wav_dir
    overwrite_output: allows ffmpeg to overwrite output, default is False
    real_time: if False, get data as soon as possible, if true wait for
               polling interval before pulling
    audio_offset: delay in seconds after stream_id before first audio
                  clip was started
    """

    def __init__(
        self,
        stream_base,
        polling_interval,
        start_unix_time,
        end_unix_time,
        wav_dir,
        overwrite_output=False,
        real_time=False,
        audio_offset=2,
    ):
        """ """

        # Get all necessary data and create index
        self.stream_base = stream_base
        self.polling_interval_in_seconds = polling_interval
        self.start_unix_time = start_unix_time
        self.end_unix_time = end_unix_time
        self.wav_dir = wav_dir
        self.overwrite_output = overwrite_output
        self.real_time = real_time
        self.is_end_of_stream = False
        self.audio_offset = audio_offset
        self.last_clip_approximate = False

        # Create wav dir if necessary
        Path(self.wav_dir).mkdir(parents=True, exist_ok=True)

        # query the stream base for all m3u8 files between the timestamps

        # split the stream base into bucket and folder
        # eg.
        # 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab' # noqa
        # would be split into s3_bucket = 'streaming-orcasound-net' and
        # folder_name = 'rpi_orcasound_lab'

        bucket_folder = self.stream_base.split(
            "https://s3-us-west-2.amazonaws.com/"
        )[1]
        tokens = bucket_folder.split("/")
        self.s3_bucket = tokens[0]
        self.folder_name = tokens[1]
        prefix = self.folder_name + "/hls/"

        # returns folder names corresponding to epochs, this grows as more
        # data is added, we should probably maintain a list of
        # hydrophone folders that exist
        all_hydrophone_folders = s3_utils.get_all_folders(
            self.s3_bucket, prefix=prefix
        )
        print(
            "Found {} folders in all for hydrophone".format(
                len(all_hydrophone_folders)
            )
        )

        self.valid_folders = s3_utils.get_folders_between_timestamp(
            all_hydrophone_folders, self.start_unix_time, self.end_unix_time
        )
        print("Found {} folders in date range".format(len(self.valid_folders)))

        self.current_folder_index = 0
        self.current_clip_start_time = self.start_unix_time

    def _advance_to_next_folder(self):
        """Move to a later session folder within the requested range."""
        self.current_folder_index += 1
        if self.current_folder_index >= len(self.valid_folders):
            self.current_folder_index = len(self.valid_folders)
            self.current_clip_start_time = self.end_unix_time
            return False

        next_folder = self.valid_folders[self.current_folder_index]
        if next_folder > self.end_unix_time:
            self.current_folder_index = len(self.valid_folders)
            self.current_clip_start_time = self.end_unix_time
            return False
        self.current_clip_start_time = max(
            self.current_clip_start_time, next_folder
        )
        return True

    def get_next_clip(self, current_clip_name=None):
        self.last_clip_approximate = False
        # Never try a prior day's playlist for a clip starting in the next
        # session folder; clips are kept within a single folder's sequence.
        while (
            self.current_folder_index + 1 < len(self.valid_folders)
            and self.current_clip_start_time
            >= self.valid_folders[self.current_folder_index + 1]
        ):
            self.current_folder_index += 1
        if self.current_folder_index >= len(self.valid_folders):
            return None, None, None

        # Get current folder
        current_folder = int(self.valid_folders[self.current_folder_index])

        # if real_time execution mode is specified
        if self.real_time:
            # sleep till enough time has elapsed

            now = datetime.utcnow()
            time_to_sleep = (current_clip_name - now).total_seconds()

            if time_to_sleep < 0:
                print("Issue with timing")

            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

        # Prefer the full archive for historical reads, with live fallback.
        archive, live = playlist_utils.load_archive_and_live(
            self.stream_base, current_folder
        )
        stream_obj = (
            archive if archive is not None and archive.segments else live
        )
        if stream_obj is None or not stream_obj.segments:
            if not self._advance_to_next_folder():
                return None, None, None
            return None, None, None
        num_total_segments = len(stream_obj.segments)
        target_duration = round(
            sum([item.duration for item in stream_obj.segments])
            / num_total_segments,
            3,
        )
        num_segments_in_wav_duration = math.ceil(
            self.polling_interval_in_seconds / target_duration
        )

        # calculate the start index by computing the current time - start of
        # current folder
        time_since_folder_start = (
            datetime_utils.get_difference_between_times_in_seconds(
                self.current_clip_start_time, current_folder
            )
        )

        # Currently there is a delay between the stream_id time and
        # the actual start of the audio stream in the folder, so add
        # an offset here to compensate.
        time_since_folder_start -= self.audio_offset

        segment_start_index = math.ceil(
            time_since_folder_start / target_duration
        )
        segment_end_index = segment_start_index + num_segments_in_wav_duration

        # Prefer timeline-based segment selection when sequence numbers or
        # program date-time metadata are available in the playlist.
        selected_segments = None
        if (
            getattr(stream_obj, "media_sequence", 0) or 0
        ) > 0 or playlist_utils.has_program_date_time(stream_obj):
            timeline = playlist_utils.get_playlist_timeline(
                stream_obj, current_folder, self.audio_offset
            )
            # Merge the live edge over the archive tail so recent segments are
            # included even while archive.m3u8 is waiting for its next upload.
            if archive is not None and live is not None:
                timeline = playlist_utils.merge_timelines(
                    playlist_utils.get_playlist_timeline(
                        archive, current_folder, self.audio_offset
                    ),
                    playlist_utils.get_playlist_timeline(
                        live, current_folder, self.audio_offset
                    ),
                )
            # Sequence gaps can be bridged only when the inferred .ts exists;
            # otherwise selection below will stop at the missing sequence.
            timeline = playlist_utils.fill_timeline_gaps(
                timeline,
                float(self.current_clip_start_time),
                float(self.current_clip_start_time)
                + self.polling_interval_in_seconds,
                playlist_utils.url_exists,
            )
            selected_segments = playlist_utils.select_segments_from_start(
                timeline,
                float(self.current_clip_start_time),
                self.polling_interval_in_seconds,
            )

        # If we cannot build a complete clip from this folder, advance to the
        # next folder and try again.
        if (
            selected_segments is None
            and segment_end_index > num_total_segments
        ) or (selected_segments is not None and not selected_segments):
            # Move forward only when a session folder remains in this range.
            if not self._advance_to_next_folder():
                return None, None, None
            return None, None, None

        # Compute the actual clip start from playlist segment indices,
        # matching HLSStream.py.  Returning the requested time
        # can be up to one polling interval (60s) off.
        if selected_segments is None:
            end_seconds = (
                segment_end_index * target_duration
                + int(current_folder)
                + self.audio_offset
            )
            actual_start_seconds = (
                end_seconds - self.polling_interval_in_seconds
            )
            segment_indices = range(segment_start_index, segment_end_index)
        else:
            actual_start_seconds = selected_segments[0].start
            segment_indices = selected_segments
        (
            clipname,
            clip_start_time,
        ) = datetime_utils.get_clip_name_from_unix_time(
            self.folder_name.replace("_", "-"),
            int(round(actual_start_seconds)),
        )

        # Can get the whole segment so update the clip_start_time for the next
        # clip
        # Create tmp path to hold .ts segments
        with TemporaryDirectory() as tmp_path:
            os.makedirs(tmp_path, exist_ok=True)

            file_names = []
            for item in segment_indices:
                audio_segment = (
                    stream_obj.segments[item]
                    if selected_segments is None
                    else item
                )
                file_name = (
                    playlist_utils.segment_filename(audio_segment)
                    if selected_segments is None
                    else audio_segment.url.rsplit("/", 1)[-1]
                )
                audio_url = (
                    playlist_utils.resolve_segment_url(audio_segment)
                    if selected_segments is None
                    else audio_segment.url
                )
                try:
                    scraper.download_from_url(audio_url, tmp_path)
                    file_names.append(file_name)
                except Exception as exc:
                    raise RuntimeError(
                        "Failed to download selected segment {}".format(
                            audio_url
                        )
                    ) from exc

            # concatentate all .ts files
            hls_file = os.path.join(tmp_path, Path(clipname + ".ts"))
            with open(hls_file, "wb") as wfd:
                for f in file_names:
                    with open(os.path.join(tmp_path, f), "rb") as fd:
                        shutil.copyfileobj(fd, wfd)

            # read the concatenated .ts and write to wav
            audio_file = clipname + ".wav"
            wav_file_path = os.path.join(self.wav_dir, audio_file)
            stream = ffmpeg.input(os.path.join(tmp_path, Path(hls_file)))
            stream = ffmpeg.output(stream, wav_file_path)
            ffmpeg.run(
                stream, overwrite_output=self.overwrite_output, quiet=True
            )

        # Advance only after every selected segment has been converted; failed
        # downloads must not yield a shorter clip or consume requested time.
        self.current_clip_start_time = (
            datetime_utils.add_interval_to_unix_time(
                self.current_clip_start_time, self.polling_interval_in_seconds
            )
        )
        if selected_segments is not None:
            self.last_clip_approximate = any(
                item.approximate for item in selected_segments
            )

        # If we're in demo mode, we need to fake timestamps to make it seem
        # like the date range is real-time
        if current_clip_name:
            clipname, _ = get_readable_clipname(
                self.folder_name.replace("_", "-"), current_clip_name
            )

            # rename wav file
            full_new_clip_path = os.path.join(self.wav_dir, clipname + ".wav")
            os.rename(wav_file_path, full_new_clip_path)
            wav_file_path = full_new_clip_path

            # change clip_start_time - this has to be in UTC so that the email
            # can be in PDT
            clip_start_time = current_clip_name.isoformat() + "Z"

        # Get new index
        return wav_file_path, clip_start_time, current_clip_name

    def is_stream_over(self):
        # returns true or false based on whether the stream is over
        return int(self.current_clip_start_time) >= int(self.end_unix_time)
