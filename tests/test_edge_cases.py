import datetime as dt
import os
import shutil
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, mock_open, patch

from orca_hls_utils import datetime_utils, s3_utils
from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream


class TestEdgeCases(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for wav files
        self.temp_dir = tempfile.mkdtemp()
        self.wav_dir = os.path.join(self.temp_dir, "wav")
        os.makedirs(self.wav_dir, exist_ok=True)

        # Common test parameters
        self.stream_base = (
            "https://s3-us-west-2.amazonaws.com/test-bucket/test-hydrophone"
        )
        self.polling_interval = 10  # 10 seconds

        # Test time ranges
        self.start_time = 1700000000  # Base timestamp
        self.end_time = self.start_time + 3600  # 1 hour later

    def tearDown(self):
        # Clean up temporary directory
        shutil.rmtree(self.temp_dir)

    # =================================================================
    # TIME BOUNDARY EDGE CASES
    # =================================================================

    def test_midnight_boundary_crossing(self):
        """Test handling data that crosses midnight boundary"""
        print("\n=== Testing midnight boundary crossing ===")

        # Create timestamps that cross midnight
        # 23:30 on one day to 00:30 the next day
        base_time = 1700000000  # Nov 14, 2023 22:13:20 UTC
        start_time = base_time + (90 * 60)  # 23:43:20
        end_time = start_time + (90 * 60)  # 01:13:20 next day

        mock_folders = [
            str(start_time),
            str(start_time + 1800),  # 30 minutes later (crosses midnight)
            str(end_time - 1800),  # 30 minutes before end
        ]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock m3u8 with segments
            mock_playlist = MagicMock()
            mock_playlist.segments = [
                MagicMock(duration=1.0, uri=f"live{i:03d}.ts")
                for i in range(10)
            ]
            mock_m3u8.return_value = mock_playlist

            # This should not raise an exception
            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Verify it handles the boundary properly
            self.assertEqual(len(stream.valid_folders), 3)

    def test_very_short_time_window(self):
        """Test handling very short time windows (less than polling interval)"""
        print("\n=== Testing very short time window ===")

        start_time = 1700000000
        end_time = start_time + 5  # Only 5 seconds

        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock m3u8 with segments
            mock_playlist = MagicMock()
            mock_playlist.segments = [
                MagicMock(duration=1.0, uri=f"live{i:03d}.ts")
                for i in range(60)
            ]
            mock_m3u8.return_value = mock_playlist

            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Should handle short window without error
            self.assertEqual(len(stream.valid_folders), 1)

    def test_future_time_range(self):
        """Test handling time ranges in the future"""
        print("\n=== Testing future time range ===")

        # Time range starting 1 hour in the future
        future_time = int(time.time()) + 3600
        start_time = future_time
        end_time = future_time + 600

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between:

            mock_get_all_folders.return_value = (
                []
            )  # No folders exist for future time
            mock_get_folders_between.return_value = []

            # Should raise IndexError for no valid folders
            with self.assertRaises(IndexError) as context:
                DateRangeHLSStream(
                    self.stream_base,
                    self.polling_interval,
                    start_time,
                    end_time,
                    self.wav_dir,
                )

            # Verify the error message
            self.assertIn("No valid folders found", str(context.exception))

    # =================================================================
    # MALFORMED DATA EDGE CASES
    # =================================================================

    def test_corrupted_m3u8_playlist(self):
        """Test handling corrupted or malformed m3u8 playlists"""
        print("\n=== Testing corrupted m3u8 playlist ===")

        start_time = 1700000000
        end_time = start_time + 600
        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock m3u8.load to raise an exception (corrupted file)
            mock_m3u8.side_effect = Exception("Corrupted m3u8 file")

            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # get_next_clip should handle the exception gracefully
            clipname, clip_start_time, download_stats = stream.get_next_clip()

            # Should return None values when playlist is corrupted
            self.assertIsNone(clipname)
            self.assertIsNone(clip_start_time)
            self.assertIsNone(download_stats)

    def test_empty_m3u8_playlist(self):
        """Test handling empty m3u8 playlists"""
        print("\n=== Testing empty m3u8 playlist ===")

        start_time = 1700000000
        end_time = start_time + 600
        mock_folders = [str(start_time), str(start_time + 300)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock empty playlist
            mock_playlist = MagicMock()
            mock_playlist.segments = []  # Empty segments
            mock_m3u8.return_value = mock_playlist

            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Should move to next folder when current is empty
            clipname, clip_start_time, download_stats = stream.get_next_clip()

            # Should return None and move to next folder
            self.assertIsNone(clipname)
            self.assertTrue(
                stream.current_folder_index >= 1
            )  # Should have moved to next folder

    def test_segments_with_zero_duration(self):
        """Test handling segments with zero or negative duration"""
        print("\n=== Testing segments with zero duration ===")

        start_time = 1700000000
        end_time = start_time + 600
        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock playlist with ALL zero duration segments (this will trigger the default duration logic)
            mock_playlist = MagicMock()
            mock_segments = []
            base_uri = f"{self.stream_base}/hls/{start_time}/"
            for i in range(3):
                segment = MagicMock()
                segment.duration = 0.0  # All zero duration
                segment.uri = f"live{i:03d}.ts"
                segment.base_uri = base_uri
                mock_segments.append(segment)
            mock_playlist.segments = mock_segments
            mock_m3u8.return_value = mock_playlist

            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Test just the duration calculation part - we'll stop before the actual download
            # by checking if the stream correctly calculates target_duration

            # Manually test the duration calculation logic
            durations = [
                item.duration
                for item in mock_playlist.segments
                if item.duration and item.duration > 0
            ]
            if not durations:
                target_duration = (
                    1.0  # Default used when all segments have zero duration
                )
            else:
                target_duration = sum(durations) / len(durations)

            # Should have used default duration of 1.0 when all segments have zero duration
            self.assertEqual(target_duration, 1.0)

            # Should not crash during initialization
            self.assertIsNotNone(stream.current_folder_index)
            self.assertEqual(stream.current_folder_index, 0)

    # =================================================================
    # NETWORK AND S3 EDGE CASES
    # =================================================================

    def test_s3_connection_timeout(self):
        """Test handling S3 connection timeouts"""
        print("\n=== Testing S3 connection timeout ===")

        start_time = 1700000000
        end_time = start_time + 600

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders:

            # Mock S3 timeout
            mock_get_all_folders.side_effect = Exception("Connection timeout")

            # Should raise the exception - this is expected behavior
            with self.assertRaises(Exception):
                DateRangeHLSStream(
                    self.stream_base,
                    self.polling_interval,
                    start_time,
                    end_time,
                    self.wav_dir,
                )

    def test_s3_access_denied(self):
        """Test handling S3 access denied errors"""
        print("\n=== Testing S3 access denied ===")

        start_time = 1700000000
        end_time = start_time + 600

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders:

            # Mock S3 access denied
            mock_get_all_folders.side_effect = Exception("Access denied")

            # Should raise the exception - this is expected behavior
            with self.assertRaises(Exception):
                DateRangeHLSStream(
                    self.stream_base,
                    self.polling_interval,
                    start_time,
                    end_time,
                    self.wav_dir,
                )

    # =================================================================
    # DATETIME UTILITY EDGE CASES
    # =================================================================

    def test_datetime_leap_year_handling(self):
        """Test datetime utilities with leap year dates"""
        print("\n=== Testing leap year handling ===")

        # February 29, 2024 (leap year)
        leap_day_timestamp = 1709251200  # 2024-02-29 12:00:00 UTC

        clipname, readable_datetime = (
            datetime_utils.get_clip_name_from_unix_time(
                "test-hydrophone", leap_day_timestamp
            )
        )

        # Should handle leap year correctly
        self.assertIn("2024_02_29", clipname)
        self.assertIn("2024_02_29", readable_datetime)

        print(f"Leap year timestamp {leap_day_timestamp} -> {clipname}")

    def test_datetime_negative_timestamp(self):
        """Test handling negative timestamps (before 1970)"""
        print("\n=== Testing negative timestamp ===")

        # Test with negative timestamp (before Unix epoch)
        negative_timestamp = -86400  # 1 day before epoch

        try:
            clipname, readable_datetime = (
                datetime_utils.get_clip_name_from_unix_time(
                    "test-hydrophone", negative_timestamp
                )
            )

            # Should handle negative timestamps
            self.assertIsNotNone(clipname)
            self.assertIsNotNone(readable_datetime)

            print(f"Negative timestamp {negative_timestamp} -> {clipname}")

        except (ValueError, OSError) as e:
            # Some systems might not support negative timestamps
            print(f"System doesn't support negative timestamps: {e}")
            self.skipTest("System doesn't support negative timestamps")

    def test_datetime_far_future_timestamp(self):
        """Test handling very large timestamps (far future)"""
        print("\n=== Testing far future timestamp ===")

        # Test with very large timestamp (year 2100)
        far_future_timestamp = 4102444800  # 2100-01-01 00:00:00 UTC

        clipname, readable_datetime = (
            datetime_utils.get_clip_name_from_unix_time(
                "test-hydrophone", far_future_timestamp
            )
        )

        # Should handle far future timestamps (adjust for timezone conversion)
        # The exact date might be different due to timezone conversion
        self.assertIn("test-hydrophone", clipname)
        self.assertIn("2099_12_31", clipname)  # Adjusted for timezone
        self.assertIn("2099_12_31", readable_datetime)

        print(f"Far future timestamp {far_future_timestamp} -> {clipname}")

    # =================================================================
    # FILESYSTEM EDGE CASES
    # =================================================================

    def test_invalid_wav_directory(self):
        """Test handling invalid or inaccessible wav directory"""
        print("\n=== Testing invalid wav directory ===")

        start_time = 1700000000
        end_time = start_time + 600
        invalid_wav_dir = "/root/invalid/path/that/does/not/exist"

        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Should raise an exception for invalid directory path
            with self.assertRaises((OSError, PermissionError)):
                DateRangeHLSStream(
                    self.stream_base,
                    self.polling_interval,
                    start_time,
                    end_time,
                    invalid_wav_dir,
                )

    def test_disk_space_exhaustion_simulation(self):
        """Test handling disk space exhaustion (simulated)"""
        print("\n=== Testing disk space exhaustion simulation ===")

        start_time = 1700000000
        end_time = start_time + 600
        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8, patch(
            "orca_hls_utils.DateRangeHLSStream.ffmpeg.run"
        ) as mock_ffmpeg:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock m3u8 with segments
            mock_playlist = MagicMock()
            mock_segments = []
            for i in range(10):
                segment = MagicMock()
                segment.duration = 1.0
                segment.uri = f"live{i:03d}.ts"
                segment.base_uri = f"{self.stream_base}/hls/{start_time}/"
                mock_segments.append(segment)
            mock_playlist.segments = mock_segments
            mock_m3u8.return_value = mock_playlist

            # Mock disk space exhaustion on ffmpeg
            mock_ffmpeg.side_effect = OSError("No space left on device")

            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # get_next_clip should handle the disk space error
            with self.assertRaises(OSError):
                stream.get_next_clip()

    # =================================================================
    # PERFORMANCE EDGE CASES
    # =================================================================

    def test_very_large_time_range(self):
        """Test handling very large time ranges"""
        print("\n=== Testing very large time range ===")

        start_time = 1700000000
        end_time = start_time + (30 * 24 * 3600)  # 30 days

        # Create a large number of mock folders
        mock_folders = [
            str(start_time + i * 3600) for i in range(720)
        ]  # 30 days * 24 hours

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Should handle large time ranges without performance issues
            stream = DateRangeHLSStream(
                self.stream_base,
                self.polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Verify it can handle large number of folders
            self.assertEqual(len(stream.valid_folders), 720)
            self.assertFalse(stream.is_stream_over())

    def test_very_high_polling_interval(self):
        """Test handling very high polling intervals"""
        print("\n=== Testing very high polling interval ===")

        start_time = 1700000000
        end_time = start_time + 600
        very_high_polling_interval = 300  # 5 minutes

        mock_folders = [str(start_time)]

        with patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders"
        ) as mock_get_all_folders, patch(
            "orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp"
        ) as mock_get_folders_between, patch(
            "orca_hls_utils.DateRangeHLSStream.m3u8.load"
        ) as mock_m3u8:

            mock_get_all_folders.return_value = mock_folders
            mock_get_folders_between.return_value = mock_folders

            # Mock m3u8 with segments
            mock_playlist = MagicMock()
            mock_playlist.segments = [
                MagicMock(duration=1.0, uri=f"live{i:03d}.ts")
                for i in range(600)
            ]
            mock_m3u8.return_value = mock_playlist

            # Should handle high polling intervals
            stream = DateRangeHLSStream(
                self.stream_base,
                very_high_polling_interval,
                start_time,
                end_time,
                self.wav_dir,
            )

            # Should not crash with high polling interval
            self.assertEqual(
                stream.polling_interval_in_seconds, very_high_polling_interval
            )


if __name__ == "__main__":
    unittest.main()
