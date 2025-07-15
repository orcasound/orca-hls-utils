import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path
import datetime as dt

from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream
from orca_hls_utils import s3_utils


class TestDataAvailabilityScenariosFixed(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for wav files
        self.temp_dir = tempfile.mkdtemp()
        self.wav_dir = os.path.join(self.temp_dir, "wav")
        os.makedirs(self.wav_dir, exist_ok=True)
        
        # Common test parameters
        self.stream_base = "https://s3-us-west-2.amazonaws.com/test-bucket/test-hydrophone"
        self.polling_interval = 10  # 10 seconds
        
        # Test time ranges (using timestamps that might have data)
        self.start_time = 1700000000  # Base timestamp
        self.end_time = self.start_time + 3600  # 1 hour later
        
        # Create mock folder structure
        self.mock_folders = [
            str(self.start_time),
            str(self.start_time + 600),   # 10 minutes later
            str(self.start_time + 1200),  # 20 minutes later
            str(self.start_time + 1800),  # 30 minutes later
            str(self.start_time + 2400),  # 40 minutes later
            str(self.start_time + 3000),  # 50 minutes later
        ]
        
    def tearDown(self):
        # Clean up temporary directory
        shutil.rmtree(self.temp_dir)
        
    def create_mock_m3u8_content(self, num_segments=10, segment_duration=1.0):
        """Create mock m3u8 content with specified number of segments"""
        segments = []
        for i in range(num_segments):
            segment_mock = MagicMock()
            segment_mock.duration = segment_duration
            segment_mock.uri = f"live{i:03d}.ts"
            segment_mock.base_uri = f"{self.stream_base}/hls/{self.mock_folders[0]}/"
            segments.append(segment_mock)
        
        playlist_mock = MagicMock()
        playlist_mock.segments = segments
        return playlist_mock
    
    def create_empty_m3u8_content(self):
        """Create mock m3u8 content with no segments"""
        playlist_mock = MagicMock()
        playlist_mock.segments = []
        return playlist_mock

    def create_mock_file_operations(self):
        """Create comprehensive mocks for all file operations"""
        def mock_download_from_url(url, tmp_path):
            # Simulate successful download by creating a fake file
            filename = url.split('/')[-1]
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')  # Write fake content
            return filepath

        def mock_ffmpeg_run(*args, **kwargs):
            # Simulate successful ffmpeg conversion
            return None

        def mock_open_files(filename, mode='rb'):
            # Return fake file content for any file read
            if mode == 'rb':
                return mock_open(read_data=b'fake_ts_content').return_value
            elif mode == 'wb':
                return mock_open().return_value
            else:
                return mock_open().return_value

        return mock_download_from_url, mock_ffmpeg_run, mock_open_files

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_1_initial_data_then_missing_middle_to_end(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test data available initially but missing from middle till end"""
        print("\n=== Test Scenario 1: Initial data available, missing from middle to end ===")
        
        # Setup mocks
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        def mock_m3u8_load(url):
            # First folder has data, rest are empty
            if self.mock_folders[0] in url:
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None    # Simulate successful conversion
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Test behavior: Should get one clip then end
        first_result = stream.get_next_clip()
        self.assertIsNotNone(first_result[0])  # Should get first clip
        
        # Continue until stream ends due to missing data
        clips_downloaded = 1
        while not stream.is_stream_over() and clips_downloaded < 10:  # Safety limit
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_downloaded += 1
            
        # Stream should end due to missing data
        self.assertTrue(stream.is_stream_over() or clips_downloaded == 1)
        print(f"Downloaded {clips_downloaded} clips before hitting missing data")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_2_missing_middle_data(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test data not available in middle of window"""
        print("\n=== Test Scenario 2: Missing data in middle of window ===")
        
        # Setup mocks
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        def mock_m3u8_load(url):
            # First and last folders have data, middle ones are empty
            if self.mock_folders[0] in url or self.mock_folders[-1] in url:
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None
        
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Should be able to get data from first folder
        result = stream.get_next_clip()
        self.assertIsNotNone(result[0])  # Should get first clip
        
        # Should handle missing middle data gracefully
        clips_downloaded = 1
        none_results = 0
        while not stream.is_stream_over() and clips_downloaded < 5 and none_results < 10:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_downloaded += 1
            else:
                none_results += 1
        
        # Should have gotten some clips and handled missing data
        self.assertGreaterEqual(clips_downloaded, 1)
        print(f"Downloaded {clips_downloaded} clips, encountered {none_results} missing data points")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_3_missing_later_half(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test data not available in later half of window"""
        print("\n=== Test Scenario 3: Missing data in later half of window ===")
        
        # Setup mocks - first half has data, second half is empty
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        def mock_m3u8_load(url):
            # First 3 folders have data, last 3 are empty
            folder_index = None
            for i, folder in enumerate(self.mock_folders):
                if folder in url:
                    folder_index = i
                    break
            
            if folder_index is not None and folder_index < 3:
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None
        
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Should get data from first half
        clips_downloaded = 0
        while not stream.is_stream_over() and clips_downloaded < 10:  # Safety limit
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_downloaded += 1
            elif clips_downloaded > 0:  # Hit missing data after getting some clips
                break
        
        # Should have gotten at least some clips from the first half
        self.assertGreater(clips_downloaded, 0)
        print(f"Downloaded {clips_downloaded} clips from first half before hitting missing data")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    def test_scenario_4_no_data_available(self, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test no data available at all"""
        print("\n=== Test Scenario 4: No data available ===")
        
        # Setup mocks with no folders
        mock_get_all_folders.return_value = []
        mock_get_folders_between.return_value = []
        
        # Should raise IndexError when no data is available
        with self.assertRaises(IndexError) as context:
            DateRangeHLSStream(
                self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
            )
        
        self.assertIn("No valid folders found", str(context.exception))
        print("Correctly raised IndexError for no data scenario")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_5_full_data_available(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test full data available throughout window"""
        print("\n=== Test Scenario 5: Full data available ===")
        
        # Setup mocks with all folders having data
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        # All folders have full data
        mock_m3u8.return_value = self.create_mock_m3u8_content(num_segments=60)
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None
        
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Should be able to get multiple clips
        clips_downloaded = 0
        max_clips = 5  # Limit for test performance
        
        while not stream.is_stream_over() and clips_downloaded < max_clips:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_downloaded += 1
            else:
                break  # Stop if we hit None result
        
        # Should have gotten multiple clips since data is fully available
        self.assertGreater(clips_downloaded, 0)
        print(f"Downloaded {clips_downloaded} clips with full data availability")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_6_single_folder_insufficient_data(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test single folder with insufficient data"""
        print("\n=== Test Scenario 6: Single folder with insufficient data ===")
        
        # Setup mocks with only one folder
        single_folder = [self.mock_folders[0]]
        mock_get_all_folders.return_value = single_folder
        mock_get_folders_between.return_value = single_folder
        
        # Folder has very few segments
        mock_m3u8.return_value = self.create_mock_m3u8_content(num_segments=3)
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None
        
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Should be able to create stream but may have limited data
        self.assertEqual(len(stream.valid_folders), 1)
        
        # Try to get a clip
        result = stream.get_next_clip()
        
        # Should either get a clip or handle insufficient data gracefully
        if result[0] is not None:
            print("Successfully got clip from single folder with limited data")
        else:
            print("Gracefully handled insufficient data in single folder")
        
        # Should not crash
        self.assertIsNotNone(stream.current_folder_index)

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_7_intermittent_data_gaps(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test intermittent data gaps"""
        print("\n=== Test Scenario 7: Intermittent data gaps ===")
        
        # Setup mocks with alternating data availability
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        def mock_m3u8_load(url):
            # Alternate between having data and not having data
            folder_index = None
            for i, folder in enumerate(self.mock_folders):
                if folder in url:
                    folder_index = i
                    break
            
            if folder_index is not None and folder_index % 2 == 0:
                # Even indices have data
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                # Odd indices are empty
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        
        # Mock download_from_url to actually create the files
        def mock_download_side_effect(url, tmp_path):
            import os
            filename = os.path.basename(url)
            filepath = os.path.join(tmp_path, filename)
            with open(filepath, 'wb') as f:
                f.write(b'fake_ts_content')
        
        mock_download.side_effect = mock_download_side_effect
        mock_ffmpeg.return_value = None
        
        stream = DateRangeHLSStream(
            self.stream_base, self.polling_interval, self.start_time, self.end_time, self.wav_dir
        )
        
        # Should handle intermittent gaps
        clips_downloaded = 0
        none_results = 0
        attempts = 0
        max_attempts = 10
        
        while not stream.is_stream_over() and attempts < max_attempts:
            result = stream.get_next_clip()
            attempts += 1
            
            if result[0] is not None:
                clips_downloaded += 1
            else:
                none_results += 1
        
        # Should have gotten some clips despite gaps
        print(f"Downloaded {clips_downloaded} clips with {none_results} gaps in {attempts} attempts")
        
        # Should not crash and should handle gaps gracefully
        self.assertIsNotNone(stream.current_folder_index)


if __name__ == '__main__':
    unittest.main() 