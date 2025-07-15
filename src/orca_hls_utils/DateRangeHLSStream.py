import datetime as dt
import logging
import math
import os
import shutil

# import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime  # , timedelta
from multiprocessing import Pool
from pathlib import Path
from tempfile import TemporaryDirectory

import ffmpeg
import m3u8

# from botocore import UNSIGNED
# from botocore.config import Config
from pytz import timezone
from tqdm import tqdm

from . import datetime_utils, s3_utils, scraper


def get_readable_clipname(hydrophone_id, cliptime_utc):
    """
    Convert a UTC datetime to a human-readable clip name with hydrophone ID.

    Args:
        hydrophone_id (str): Identifier for the hydrophone (e.g., "rpi-orcasound-lab")
        cliptime_utc (datetime): UTC datetime to convert to clip name

    Returns:
        tuple: (clipname (str), date (datetime))
            - clipname: Formatted string like "rpi-orcasound-lab_2020_09_27_00_16_55_PDT"
            - date: Datetime converted to US/Pacific timezone

    Example:
        >>> clipname, date = get_readable_clipname("rpi-orcasound-lab", datetime(2020, 9, 27, 7, 16, 55))
        >>> print(clipname)
        "rpi-orcasound-lab_2020_09_27_00_16_55_PDT"
    """
    # cliptime is of the form 2020-09-27T00/16/55.677242Z
    cliptime_utc = timezone("UTC").localize(cliptime_utc)
    date = cliptime_utc.astimezone(timezone("US/Pacific"))
    date_format = "%Y_%m_%d_%H_%M_%S_%Z"
    clipname = date.strftime(date_format)
    return hydrophone_id + "_" + clipname, date


# TODO: Handle date ranges that don't exist
class DateRangeHLSStream:
    """
    Main class for processing HLS (HTTP Live Streaming) audio data from Orcasound hydrophones
    within a specified date range.

    This class handles:
    - Discovering available audio folders in S3 buckets within date ranges
    - Downloading and processing HLS segments (.ts files)
    - Converting segments to WAV files using FFmpeg
    - Managing real-time vs. batch processing modes

    Attributes:
        stream_base (str): Base URL for HLS stream (e.g., 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab')
        polling_interval_in_seconds (int): Duration of each audio clip in seconds (typically 60)
        start_unix_time (int): Start timestamp for processing (Unix timestamp)
        end_unix_time (int): End timestamp for processing (Unix timestamp)
        wav_dir (str): Directory path where WAV files will be saved
        overwrite_output (bool): Whether FFmpeg should overwrite existing output files
        quiet_ffmpeg (bool): Whether to suppress FFmpeg console output
        real_time (bool): If True, wait for polling intervals; if False, process as fast as possible
    """

    def __init__(
        self,
        stream_base,
        polling_interval,
        start_unix_time,
        end_unix_time,
        wav_dir,
        overwrite_output=False,
        quiet_ffmpeg=False,
        real_time=False,
    ):
        """
        Initialize DateRangeHLSStream with stream parameters and discover available data.

        Args:
            stream_base (str): Base URL for HLS stream
            polling_interval (int): Duration of each clip in seconds
            start_unix_time (int): Start timestamp (Unix time)
            end_unix_time (int): End timestamp (Unix time)
            wav_dir (str): Output directory for WAV files
            overwrite_output (bool, optional): Allow FFmpeg to overwrite files. Defaults to False.
            quiet_ffmpeg (bool, optional): Suppress FFmpeg output. Defaults to False.
            real_time (bool, optional): Enable real-time processing mode. Defaults to False.

        Raises:
            IndexError: If no valid folders found in the specified date range
            OSError: If wav_dir cannot be created
            PermissionError: If insufficient permissions to create wav_dir

        Side Effects:
            - Creates wav_dir if it doesn't exist
            - Queries S3 to discover available data folders
            - Sets up internal state for clip processing
        """

        self.logger = logging.getLogger("DateRangeHLSStream")

        # Get all necessary data and create index
        self.stream_base = stream_base
        self.polling_interval_in_seconds = polling_interval
        self.start_unix_time = start_unix_time
        self.end_unix_time = end_unix_time
        self.wav_dir = wav_dir
        self.overwrite_output = overwrite_output
        self.real_time = real_time
        self.is_end_of_stream = False
        self.quiet_ffmpeg = quiet_ffmpeg

        # Create wav dir if necessary
        try:
            Path(self.wav_dir).mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            self.logger.error(
                f"Cannot create wav directory {self.wav_dir}: {e}"
            )
            raise

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
        self.logger.info(
            "Found {} folders in all for hydrophone".format(
                len(all_hydrophone_folders)
            )
        )

        self.valid_folders = s3_utils.get_folders_between_timestamp(
            all_hydrophone_folders, self.start_unix_time, self.end_unix_time
        )

        if not self.valid_folders:
            raise IndexError(
                f"No valid folders found in date range {self.start_unix_time} to {self.end_unix_time}"
            )

        self.logger.info(
            f"Found {len(self.valid_folders)} folders in date range. Starting: {self.valid_folders[0]} - Ending: {self.valid_folders[-1]}"
        )

        self.current_folder_index = 0
        self.current_clip_start_time = self.start_unix_time

    def process_segment(self, args):
        """
        Download and process a single HLS segment (.ts file) in parallel.

        Args:
            args (tuple): Contains (base_path, file_name, tmp_path, logger)
                - base_path (str): Base URL for the segment
                - file_name (str): Name of the .ts file to download
                - tmp_path (str): Temporary directory path for downloads
                - logger: Logger instance for status messages

        Returns:
            str or None:
                - file_name if download successful
                - None if download failed

        Side Effects:
            - Downloads .ts file to tmp_path
            - Logs debug/warning messages
        """
        base_path, file_name, tmp_path, logger = args
        audio_url = base_path + file_name
        try:
            scraper.download_from_url(audio_url, tmp_path)
            logger.debug(f"Adding file {file_name}")
            return file_name
        except Exception:
            logger.warning(f"Skipping {audio_url}: error.")
            return None

    def get_next_clip(self, current_clip_name=None):
        """
        Retrieve and process the next audio clip from the HLS stream.

        This is the main method for sequential clip processing. It:
        1. Loads M3U8 playlist for current time window
        2. Calculates which segments to download
        3. Downloads segments in parallel
        4. Concatenates and converts to WAV format

        Args:
            current_clip_name (datetime, optional): For real-time mode, the expected clip time.
                Used to calculate sleep intervals for real-time processing.

        Returns:
            tuple: (wav_file_path, clip_start_time, current_clip_name)
                - wav_file_path (str): Path to generated WAV file, or None if no data
                - clip_start_time (str): ISO format timestamp of clip start, or None
                - current_clip_name (datetime): Processed clip timestamp, or None

        Side Effects:
            - Updates internal state (current_folder_index, current_clip_start_time)
            - May sleep in real-time mode
            - Creates temporary files and cleans them up
            - Writes WAV file to wav_dir
            - Sets is_end_of_stream flag when processing complete

        Examples:
            # Sequential processing
            while not stream.is_stream_over():
                wav_path, start_time, clip_name = stream.get_next_clip()
                if wav_path:
                    print(f"Generated: {wav_path}")
        """
        # Check if we've run out of folders
        if self.current_folder_index >= len(self.valid_folders):
            self.is_end_of_stream = True
            return None, None, None

        # Get current folder
        current_folder = int(self.valid_folders[self.current_folder_index])
        (
            clipname,
            clip_start_time,
        ) = datetime_utils.get_clip_name_from_unix_time(
            self.folder_name.replace("_", "-"), self.current_clip_start_time
        )

        # if real_time execution mode is specified
        if self.real_time and current_clip_name is not None:
            # sleep till enough time has elapsed

            now = dt.datetime.now(dt.timezone.utc)
            time_to_sleep = (current_clip_name - now).total_seconds()

            if time_to_sleep < 0:
                self.logger.warning("Issue with timing")

            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

        # read in current m3u8 file
        # stream_url for the current AWS folder
        stream_url = "{}/hls/{}/live.m3u8".format(
            (self.stream_base), (current_folder)
        )

        try:
            stream_obj = m3u8.load(stream_url)
        except Exception as e:
            self.logger.error(
                f"Failed to load m3u8 playlist from {stream_url}: {e}"
            )
            # Move to next folder or end stream
            if self.current_folder_index + 1 >= len(self.valid_folders):
                self.is_end_of_stream = True
                return None, None, None
            self.current_folder_index += 1
            if self.current_folder_index >= len(self.valid_folders):
                self.is_end_of_stream = True
                return None, None, None
            self.current_clip_start_time = int(
                self.valid_folders[self.current_folder_index]
            )
            return None, None, None

        num_total_segments = len(stream_obj.segments)
        if num_total_segments == 0:
            # No segments in this folder, try next folder or end stream
            if self.current_folder_index + 1 >= len(self.valid_folders):
                self.is_end_of_stream = True
                return None, None, None
            self.current_folder_index += 1
            if self.current_folder_index >= len(self.valid_folders):
                self.is_end_of_stream = True
                return None, None, None
            self.current_clip_start_time = int(
                self.valid_folders[self.current_folder_index]
            )
            return None, None, None

        # Calculate target duration with protection against zero/None durations
        durations = [
            item.duration
            for item in stream_obj.segments
            if item.duration and item.duration > 0
        ]
        if not durations:
            # All segments have zero or None duration, use default
            self.logger.warning(
                f"All segments have zero or None duration in {stream_url}, using default 1.0 seconds"
            )
            target_duration = 1.0
        else:
            target_duration = sum(durations) / len(durations)
        num_segments_in_wav_duration = math.ceil(
            self.polling_interval_in_seconds / target_duration
        )

        # calculate the start index by computing the current time - start of
        # current folder
        segment_start_index = math.ceil(
            datetime_utils.get_difference_between_times_in_seconds(
                self.current_clip_start_time, current_folder
            )
            / target_duration
        )
        segment_end_index = segment_start_index + num_segments_in_wav_duration

        if segment_end_index > num_total_segments:
            if self.current_folder_index + 1 >= len(self.valid_folders):
                # Something went wrong, we'll just return the current data
                self.logger.warning("Missing data, returning truncated file.")
                self.logger.debug(f"Start index is {segment_start_index}")
                self.logger.debug(
                    f"Adjusting end index from {segment_end_index} to {num_total_segments}"
                )
                segment_end_index = num_total_segments
                if segment_end_index < segment_start_index:
                    self.logger.warning("No data found")
                    self.current_clip_start_time = self.end_unix_time
                    return None, None, None
            else:
                # move to the next folder and increment the
                # current_clip_start_time to the new
                self.current_folder_index += 1
                if self.current_folder_index >= len(self.valid_folders):
                    self.is_end_of_stream = True
                    return None, None, None
                self.current_clip_start_time = int(
                    self.valid_folders[self.current_folder_index]
                )
                return None, None, None

        # Can get the whole segment so update the clip_start_time for the next
        # clip
        # We do this before we actually do the pulling in case there is a
        # problem with this clip
        self.current_clip_start_time = (
            datetime_utils.add_interval_to_unix_time(
                self.current_clip_start_time, self.polling_interval_in_seconds
            )
        )

        # Create tmp path to hold .ts segments
        with TemporaryDirectory() as tmp_path:
            os.makedirs(tmp_path, exist_ok=True)

            # Use a multiprocessing Pool to download and process segments in parallel
            args_list = [
                (
                    audio_segment.base_uri,
                    audio_segment.uri,
                    tmp_path,
                    self.logger,
                )
                for audio_segment in stream_obj.segments[
                    segment_start_index:segment_end_index
                ]
            ]
            file_names = []
            with ThreadPoolExecutor() as executor:
                # Submit tasks to the executor
                futures = [
                    executor.submit(self.process_segment, args)
                    for args in args_list
                ]
                # Wait for all futures to complete
                for future in futures:
                    result = future.result()
                    if result is not None:
                        file_names.append(result)

            file_names = [
                f for f in file_names if f is not None
            ]  # Filter out None results

            # concatentate all .ts files
            self.logger.info(f"Files to concat = {file_names}")
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
            try:
                ffmpeg.run(
                    stream,
                    overwrite_output=self.overwrite_output,
                    quiet=self.quiet_ffmpeg,
                )
            except Exception as e:
                shutil.copyfile(hls_file, "ts/badfile.ts")
                raise e

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
        """
        Check if all available data in the date range has been processed.

        Returns:
            bool: True if processing is complete, False if more data available
                - True when current time >= end_unix_time OR is_end_of_stream flag set
                - False if more clips can be processed

        Example:
            while not stream.is_stream_over():
                wav_path, start_time, clip_name = stream.get_next_clip()
                # Process clip...
        """
        # returns true or false based on whether the stream is over
        return (
            int(self.current_clip_start_time) >= int(self.end_unix_time)
            or self.is_end_of_stream
        )

    def get_all_clips(self):
        """
        Download and process all audio clips in the date range using parallel processing.

        This method provides batch processing of all clips rather than sequential processing.
        It's more efficient for large date ranges as it processes multiple folders in parallel.

        Returns:
            tuple: (wav_file_paths, clip_start_times)
                - wav_file_paths (list[str]): List of paths to generated WAV files
                - clip_start_times (list[str]): List of ISO format timestamps for each clip

        Side Effects:
            - Downloads all segments for the entire date range
            - Creates multiple WAV files in wav_dir
            - Uses multiprocessing for parallel folder processing
            - Updates internal stream state

        Example:
            wav_paths, start_times = stream.get_all_clips()
            for path, time in zip(wav_paths, start_times):
                print(f"Clip {time}: {path}")
        """
        segment_indexes = self.setup_download_variables()
        segment_indexes_flattened = [
            x for xs in list(segment_indexes.values()) for x in xs
        ]
        with TemporaryDirectory() as tmp_path:
            args_list = [
                (obj[2], obj[0], obj[1], obj[3], tmp_path, self.logger)
                for obj in segment_indexes_flattened
            ]
            with Pool() as pool:
                wav_file_paths_and_clip_start_times = pool.map(
                    self.download_clips_from_folder, args_list
                )

            wav_file_paths = [
                f[0]
                for f in wav_file_paths_and_clip_start_times
                if f[0] is not None
            ]
            clip_start_times = [
                f[1]
                for f in wav_file_paths_and_clip_start_times
                if f[1] is not None
            ]
        return wav_file_paths, clip_start_times

    def download_clips_from_folder(self, args):
        """
        Download and process clips from a single S3 folder (used in parallel processing).

        Args:
            args (tuple): Contains processing parameters:
                - current_clip_start_time (int): Unix timestamp for clip start
                - segment_start_index (int): First segment index to download
                - segment_end_index (int): Last segment index to download
                - stream_obj: M3U8 stream object with segment information
                - tmp_path (str): Temporary directory for file operations
                - logger: Logger instance for status messages

        Returns:
            tuple: (wav_file_path, clip_start_time)
                - wav_file_path (str): Path to generated WAV file, or None if failed
                - clip_start_time (str): ISO format timestamp, or None if failed

        Side Effects:
            - Downloads multiple .ts segments from S3
            - Concatenates segments into single file
            - Converts to WAV format using FFmpeg
            - Creates files in wav_dir
        """
        (
            current_clip_start_time,
            segment_start_index,
            segment_end_index,
            stream_obj,
            tmp_path,
            logger,
        ) = args
        clipname, clip_start_time = (
            datetime_utils.get_clip_name_from_unix_time(
                self.folder_name.replace("_", "-"), current_clip_start_time
            )
        )
        path_to_save = os.path.join(
            tmp_path, stream_obj.base_uri.split("/")[-2]
        )
        os.makedirs(path_to_save, exist_ok=True)

        file_names = []
        for i in range(segment_start_index, segment_end_index):
            audio_segment = stream_obj.segments[i]
            audio_url = audio_segment.base_uri + audio_segment.uri
            try:
                scraper.download_from_url(audio_url, path_to_save)
                file_names.append(audio_segment.uri)
                logger.debug(f"Adding file {audio_segment.uri}")
            except Exception as e:
                logger.warning(f"Skipping {audio_url}: error. {e}")

        logger.info(f"Files to concat = {file_names}")
        hls_file = os.path.join(path_to_save, f"{clipname}.ts")
        self.concatenate_files(file_names, path_to_save, hls_file)
        wav_file_path = self.convert_ts_to_wav(hls_file, clipname)

        return wav_file_path, clip_start_time

    def concatenate_files(self, file_names, directory, output_file):
        """
        Concatenate multiple .ts segment files into a single file.

        Args:
            file_names (list[str]): List of .ts filenames to concatenate
            directory (str): Directory containing the .ts files
            output_file (str): Full path for the concatenated output file

        Side Effects:
            - Reads all input .ts files in sequence
            - Writes binary content to output_file
            - Creates output_file in the filesystem

        Note:
            This performs simple binary concatenation, which works for MPEG-TS format.
            The files must be compatible segments from the same stream.
        """
        with open(output_file, "wb") as wfd:
            for file_name in file_names:
                file_path = os.path.join(directory, file_name)
                with open(file_path, "rb") as fd:
                    shutil.copyfileobj(fd, wfd)

    def convert_ts_to_wav(self, input_file, clipname):
        """
        Convert a MPEG-TS file to WAV format using FFmpeg.

        Args:
            input_file (str): Path to input .ts file
            clipname (str): Base name for output file (without extension)

        Returns:
            str: Full path to generated WAV file

        Raises:
            Exception: If FFmpeg conversion fails

        Side Effects:
            - Creates WAV file in self.wav_dir
            - Uses FFmpeg with configured quiet/overwrite settings
            - On error, copies input file to "ts/badfile.ts" for debugging

        Example:
            wav_path = stream.convert_ts_to_wav("/tmp/audio.ts", "clip_001")
            # Creates: {wav_dir}/clip_001.wav
        """
        audio_file = f"{clipname}.wav"
        wav_file_path = os.path.join(self.wav_dir, audio_file)
        stream = ffmpeg.input(input_file)
        stream = ffmpeg.output(stream, wav_file_path)
        try:
            ffmpeg.run(
                stream,
                quiet=self.quiet_ffmpeg,
                overwrite_output=self.overwrite_output,
            )
        except Exception as e:
            bad_file_path = os.path.join("ts", "badfile.ts")
            shutil.copyfile(input_file, bad_file_path)
            raise e
        return wav_file_path

    def setup_download_variables(self):
        """
        Pre-calculate segment indices and stream objects for batch processing.

        This method analyzes all M3U8 playlists in the date range to determine:
        - Which segments need to be downloaded from each folder
        - Timing calculations for each clip
        - Stream objects for parallel processing

        Returns:
            dict: Mapping of folder names to segment information
                Key (str): Folder timestamp
                Value (list): List of tuples containing:
                    - (segment_start_index, segment_end_index, clip_start_time, stream_obj)

        Side Effects:
            - Loads all M3U8 playlists in the date range
            - Updates internal state (current_folder_index, current_clip_start_time)
            - Logs progress information

        Note:
            This method enables efficient batch processing by pre-calculating
            all the work that needs to be done across the entire date range.
        """
        # Setup list of current_folders and current_clip_start_times
        segments_in_wav_duration = []
        target_durations = []
        segment_indexes = {}
        stream_objects = []

        for folder in tqdm(self.valid_folders):
            stream_url = "{}/hls/{}/live.m3u8".format(self.stream_base, folder)
            stream_obj = m3u8.load(stream_url)
            stream_objects.append(stream_obj)

            num_total_segments = len(stream_obj.segments)
            durations = [
                item.duration
                for item in stream_obj.segments
                if item.duration and item.duration > 0
            ]
            if not durations:
                # All segments have zero or None duration, use default
                self.logger.warning(
                    f"All segments have zero or None duration in {stream_url}, using default 1.0 seconds"
                )
                target_duration = 1.0
            else:
                target_duration = sum(durations) / len(durations)
            target_durations.append(target_duration)

            num_segments_in_wav_duration = math.ceil(
                self.polling_interval_in_seconds / target_duration
            )
            segments_in_wav_duration.append(num_segments_in_wav_duration)

        counter = 0
        while not self.is_stream_over():
            counter += 1
            current_folder = self.valid_folders[self.current_folder_index]
            segment_start_index = math.ceil(
                datetime_utils.get_difference_between_times_in_seconds(
                    self.current_clip_start_time, current_folder
                )
                / target_durations[self.current_folder_index]
            )
            segment_end_index = (
                segment_start_index
                + segments_in_wav_duration[self.current_folder_index]
            )

            if segment_end_index > num_total_segments:
                segment_end_index = num_total_segments
                if self.current_folder_index + 1 >= len(self.valid_folders):
                    pass
                else:
                    self.current_folder_index += 1
                    self.current_clip_start_time = self.valid_folders[
                        self.current_folder_index
                    ]

            if (
                self.valid_folders[self.current_folder_index]
                not in segment_indexes.keys()
            ):
                segment_indexes[
                    self.valid_folders[self.current_folder_index]
                ] = [
                    (
                        segment_start_index,
                        segment_end_index,
                        self.current_clip_start_time,
                        stream_objects[self.current_folder_index],
                    )
                ]

                self.current_clip_start_time = (
                    datetime_utils.add_interval_to_unix_time(
                        self.current_clip_start_time,
                        self.polling_interval_in_seconds,
                    )
                )
                continue
            segment_indexes[
                self.valid_folders[self.current_folder_index]
            ].append(
                (
                    segment_start_index,
                    segment_end_index,
                    self.current_clip_start_time,
                    stream_objects[self.current_folder_index],
                )
            )

            self.current_clip_start_time = (
                datetime_utils.add_interval_to_unix_time(
                    self.current_clip_start_time,
                    self.polling_interval_in_seconds,
                )
            )
        self.logger.info(f"Ran {counter} iterations")
        return segment_indexes
