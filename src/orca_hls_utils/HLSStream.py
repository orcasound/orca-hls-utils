# HLSStream class
import math
import os
import shutil
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import ffmpeg  # ffmpeg-python
import m3u8
from botocore import UNSIGNED
from botocore.config import Config
from pytz import timezone

from . import datetime_utils, scraper


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

    def __init__(self, stream_base, polling_interval, wav_dir):
        self.stream_base = stream_base
        self.polling_interval = polling_interval
        self.wav_dir = wav_dir
        bucket_folder = self.stream_base.split(
            "https://s3-us-west-2.amazonaws.com/"
        )[1]
        tokens = bucket_folder.split("/")
        self.s3_bucket = tokens[0]
        self.hydrophone_id = tokens[1]

    # this function grabs audio from last_end_time to
    def get_next_clip(self, current_clip_end_time):
        print("DEBUG get_next_clip() current_clip_end_time         : ", current_clip_end_time)
        # if current time < current_clip_end_time, sleep for the difference
        now = datetime.utcnow()
        print("DEBUG get_next_clip() now                           : ", now)

        # the extra 10 seconds to sleep is to download the last .ts segment
        # properly
        time_to_sleep = (current_clip_end_time - now).total_seconds() + 10

        if time_to_sleep > 0:
            print("DEBUG get_next_clip() time_to_sleep                 : ", time_to_sleep)
            time.sleep(time_to_sleep)

        # get latest AWS bucket
        print("Listening to location {loc}".format(loc=self.stream_base))
        latest = f"{self.stream_base}/latest.txt"
        stream_id = (
            urllib.request.urlopen(latest)
            .read()
            .decode("utf-8")
            .replace("\n", "")
        )
        print("DEBUG get_next_clip() stream_id                     : ", stream_id)

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
        print("DEBUG get_next_clip() num_total_segments            : ", num_total_segments)
        target_duration = round(
            sum([item.duration for item in stream_obj.segments])
            / num_total_segments,
            3
        )
        print("DEBUG get_next_clip() target_duration               : ", target_duration)
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
        print("DEBUG get_next_clip() current_clip_end_time_unix_pst: ", current_clip_end_time_unix_pst)
        time_since_folder_start = (
            datetime_utils.get_difference_between_times_in_seconds(
                current_clip_end_time_unix_pst, stream_id
            )
        )
        print("DEBUG get_next_clip() time_since_folder_start       : ", time_since_folder_start)
        if time_since_folder_start < self.polling_interval + 20:
            # This implies that possibly a new folder was created
            # and we do not have enough data for a 1 minute clip + 20 second
            # buffer
            # we exit and try again after hls polling interval
            None, None, current_clip_end_time

        print("DEBUG get_next_clip() fraction                      : ", (time_since_folder_start / target_duration))
        min_num_total_segments_required = math.ceil(
            time_since_folder_start / target_duration
        )
        print("DEBUG get_next_clip() num_num_total_segments_required: ", min_num_total_segments_required)
        segment_start_index = (
            min_num_total_segments_required - num_segments_in_wav_duration
        )
        print("DEBUG get_next_clip() segment_start_index           : ", segment_start_index)
        segment_end_index = segment_start_index + num_segments_in_wav_duration
        print("DEBUG get_next_clip() segment_end_index             : ", segment_end_index)

        # Compute nominal start time
        start_seconds = segment_start_index * target_duration + int(stream_id)
        print("DEBUG get_next_clip() start_seconds                 : ", start_seconds)
        start_pst = datetime.fromtimestamp(start_seconds)
        print("DEBUG get_next_clip() start_pst                     : ", start_pst)

        # Compute nominal end time
        end_seconds = segment_end_index * target_duration + int(stream_id)
        print("DEBUG get_next_clip() end_seconds                   : ", end_seconds)
        end_pst = datetime.fromtimestamp(end_seconds)
        print("DEBUG get_next_clip() end_pst                       : ", end_pst)
        end_utc = datetime.utcfromtimestamp(end_seconds)
        print("DEBUG get_next_clip() end_utc                       : ", end_utc)
        current_clip_end_time = end_utc

        if segment_end_index > num_total_segments:
            print("DEBUG not enough segments")
            return None, None, current_clip_end_time

        # Create tmp path to hold .ts segments
        tmp_path = "tmp_path"
        os.makedirs(tmp_path, exist_ok=True)

        file_names = []
        for i in range(segment_start_index, segment_end_index):
            audio_segment = stream_obj.segments[i]
            base_path = audio_segment.base_uri
            file_name = audio_segment.uri
            audio_url = base_path + file_name
            try:
                scraper.download_from_url(audio_url, tmp_path)
                file_names.append(file_name)
            except Exception:
                print("Skipping", audio_url, ": error.")

        current_clip_start_time = current_clip_end_time - timedelta(0, 60)
        print("DEBUG get_next_clip() current_clip_end_time         : ", current_clip_end_time)
        clip_start_time = current_clip_start_time.isoformat() + "Z"
        print("DEBUG get_next_clip() clip_start_time               : ", clip_start_time)
        clipname, _ = get_readable_clipname(
            self.hydrophone_id, current_clip_start_time
        )
        print("DEBUG get_next_clip() clipname                      : ", clipname)

        # concatentate all .ts files with ffmpeg
        hls_file = clipname + ".ts"
        audio_file = clipname + ".wav"
        wav_file_path = os.path.join(self.wav_dir, audio_file)
        hls_file_path = os.path.join(tmp_path, hls_file)

        # Combine .ts files into one .ts file
        print("DEBUG get_next_clip() datetime.utcnow()             : ", datetime.utcnow())
        with open(hls_file_path, 'wb') as outfile:
            for fname in file_names:
                segment_path = os.path.join(tmp_path, fname)
                print("DEBUG get_next_clip() segment_path                  : ", segment_path)
                with open(segment_path, 'rb') as infile:
                    outfile.write(infile.read())
        print("DEBUG get_next_clip() datetime.utcnow()             : ", datetime.utcnow())

        # read the concatenated .ts and write to wav
        print("DEBUG get_next_clip() writing to hls_file           : ", hls_file)
        try:
            stream = ffmpeg.input(os.path.join(tmp_path, Path(hls_file)))
            stream = ffmpeg.output(stream, wav_file_path)
            ffmpeg.run(stream, quiet=True)
        except ffmpeg.Error as e:
            print("FFmpeg command failed.")
            print("stdout:", e.stdout.decode('utf8', errors='ignore'))
            print("stderr:", e.stderr.decode('utf8', errors='ignore'))
            raise

        # clear the tmp_path
        print("DEBUG get_next_clip() clearing the tmp_path         : ", tmp_path)
        shutil.rmtree(tmp_path, ignore_errors=True)

        return wav_file_path, clip_start_time, current_clip_end_time

    def is_stream_over(self):
        # returns true or false based on whether the stream is over
        # for a live stream, the stream is never over
        return False
