import unittest
from unittest.mock import patch, MagicMock, mock_open
import tempfile
import os
import shutil
from pathlib import Path
import datetime as dt

from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream
from orca_hls_utils import s3_utils


class TestDataAvailabilityScenarios(unittest.TestCase):
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
        
        stream_mock = MagicMock()
        stream_mock.segments = segments
        return stream_mock

    def create_empty_m3u8_content(self):
        """Create mock m3u8 content with no segments"""
        stream_mock = MagicMock()
        stream_mock.segments = []
        return stream_mock

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    def test_scenario_1_initial_data_then_missing_middle_to_end(self, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 1: Data available initially but missing from middle till end"""
        print("\n=== Test Scenario 1: Initial data available, missing from middle to end ===")
        
        # Setup: Only first 2 folders have data
        available_folders = self.mock_folders[:2]
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = available_folders
        
        # First folder has data, second folder has data, rest would be missing
        def mock_m3u8_load(url):
            if any(folder in url for folder in available_folders):
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test the logic without actually processing files
        # Check initial state
        self.assertEqual(stream.current_folder_index, 0)
        self.assertFalse(stream.is_stream_over())
        
        # Test what happens when we try to get clips
        # The first call should work (we have data in first folder)
        current_folder = int(stream.valid_folders[stream.current_folder_index])
        stream_url = f"{stream.stream_base}/hls/{current_folder}/live.m3u8"
        stream_obj = mock_m3u8(stream_url)
        
        # Should have segments
        self.assertGreater(len(stream_obj.segments), 0)
        
        # Test stream termination when running out of folders
        # Move to the end of valid folders
        stream.current_folder_index = len(stream.valid_folders) - 1
        
        # Test that we can detect when folders are exhausted
        # This should trigger the boundary condition
        stream.current_folder_index = len(stream.valid_folders)
        
        # Now stream should be over
        self.assertTrue(stream.is_stream_over())
        
        print(f"Successfully tested boundary conditions with {len(available_folders)} available folders")
        print(f"Stream correctly terminates when running out of data")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_2_missing_middle_data(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 2: Data not available in middle of window"""
        print("\n=== Test Scenario 2: Missing data in middle of window ===")
        
        # Setup: First, last folders have data, middle folders are missing
        available_folders = [self.mock_folders[0], self.mock_folders[-1]]
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = available_folders
        
        def mock_m3u8_load(url):
            if any(folder in url for folder in available_folders):
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        mock_download.return_value = True
        mock_ffmpeg.return_value = None
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test getting clips
        clips_retrieved = 0
        clips_data = []
        
        while not stream.is_stream_over() and clips_retrieved < 10:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_retrieved += 1
                clips_data.append(result)
                print(f"Retrieved clip {clips_retrieved}: {result[0]}")
            else:
                print("Encountered gap in data")
                break
        
        # Assertions
        self.assertGreater(clips_retrieved, 0, "Should retrieve clips from available folders")
        print(f"Retrieved {clips_retrieved} clips despite missing middle data")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_3_missing_later_half(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 3: Data not available in later half of window"""
        print("\n=== Test Scenario 3: Missing data in later half of window ===")
        
        # Setup: First half of folders have data, second half is missing
        available_folders = self.mock_folders[:len(self.mock_folders)//2]
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = available_folders
        
        def mock_m3u8_load(url):
            if any(folder in url for folder in available_folders):
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        mock_download.return_value = True
        mock_ffmpeg.return_value = None
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test getting clips
        clips_retrieved = 0
        clips_data = []
        
        while not stream.is_stream_over() and clips_retrieved < 10:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_retrieved += 1
                clips_data.append(result)
                print(f"Retrieved clip {clips_retrieved}: {result[0]}")
            else:
                print("Hit missing data in later half")
                break
        
        # Assertions
        self.assertGreater(clips_retrieved, 0, "Should retrieve clips from first half")
        self.assertTrue(stream.is_stream_over(), "Stream should end when hitting missing data")
        print(f"Retrieved {clips_retrieved} clips from first half before hitting missing data")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    def test_scenario_4_no_data_available(self, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 4: No data available at all"""
        print("\n=== Test Scenario 4: No data available ===")
        
        # Setup: No folders available
        mock_get_all_folders.return_value = []
        mock_get_folders_between.return_value = []
        
        # This should raise an exception during initialization
        with self.assertRaises(IndexError):
            stream = DateRangeHLSStream(
                self.stream_base, 
                self.polling_interval, 
                self.start_time, 
                self.end_time, 
                self.wav_dir
            )

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_5_full_data_available(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 5: Full data available throughout window"""
        print("\n=== Test Scenario 5: Full data available ===")
        
        # Setup: All folders have data
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = self.mock_folders
        
        # All folders have data
        mock_m3u8.return_value = self.create_mock_m3u8_content(num_segments=10)
        mock_download.return_value = True
        mock_ffmpeg.return_value = None
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test getting clips
        clips_retrieved = 0
        clips_data = []
        
        while not stream.is_stream_over() and clips_retrieved < 20:  # Higher limit for full data
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_retrieved += 1
                clips_data.append(result)
                print(f"Retrieved clip {clips_retrieved}: {result[0]}")
            else:
                print("Unexpected gap in data")
                break
        
        # Assertions
        self.assertGreater(clips_retrieved, 0, "Should retrieve clips from all folders")
        print(f"Retrieved {clips_retrieved} clips from full data set")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_6_single_folder_insufficient_data(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 6: Single folder with insufficient data"""
        print("\n=== Test Scenario 6: Single folder with insufficient data ===")
        
        # Setup: Only one folder with minimal data
        available_folders = [self.mock_folders[0]]
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = available_folders
        
        # Single folder with only 2 segments (insufficient for most requests)
        mock_m3u8.return_value = self.create_mock_m3u8_content(num_segments=2)
        mock_download.return_value = True
        mock_ffmpeg.return_value = None
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test getting clips
        clips_retrieved = 0
        clips_data = []
        
        while not stream.is_stream_over() and clips_retrieved < 5:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_retrieved += 1
                clips_data.append(result)
                print(f"Retrieved clip {clips_retrieved}: {result[0]}")
            else:
                print("Hit insufficient data")
                break
        
        # Assertions
        self.assertTrue(stream.is_stream_over(), "Stream should end due to insufficient data")
        print(f"Retrieved {clips_retrieved} clips from insufficient data set")

    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_all_folders')
    @patch('orca_hls_utils.DateRangeHLSStream.s3_utils.get_folders_between_timestamp')
    @patch('orca_hls_utils.DateRangeHLSStream.m3u8.load')
    @patch('orca_hls_utils.DateRangeHLSStream.scraper.download_from_url')
    @patch('orca_hls_utils.DateRangeHLSStream.ffmpeg.run')
    def test_scenario_7_intermittent_data_gaps(self, mock_ffmpeg, mock_download, mock_m3u8, mock_get_folders_between, mock_get_all_folders):
        """Test case 7: Intermittent data gaps"""
        print("\n=== Test Scenario 7: Intermittent data gaps ===")
        
        # Setup: Alternating folders with and without data
        available_folders = [self.mock_folders[i] for i in [0, 2, 4]]  # Every other folder
        mock_get_all_folders.return_value = self.mock_folders
        mock_get_folders_between.return_value = available_folders
        
        def mock_m3u8_load(url):
            if any(folder in url for folder in available_folders):
                return self.create_mock_m3u8_content(num_segments=10)
            else:
                return self.create_empty_m3u8_content()
        
        mock_m3u8.side_effect = mock_m3u8_load
        mock_download.return_value = True
        mock_ffmpeg.return_value = None
        
        # Create stream
        stream = DateRangeHLSStream(
            self.stream_base, 
            self.polling_interval, 
            self.start_time, 
            self.end_time, 
            self.wav_dir
        )
        
        # Test getting clips
        clips_retrieved = 0
        clips_data = []
        
        while not stream.is_stream_over() and clips_retrieved < 15:
            result = stream.get_next_clip()
            if result[0] is not None:
                clips_retrieved += 1
                clips_data.append(result)
                print(f"Retrieved clip {clips_retrieved}: {result[0]}")
            else:
                print("Hit data gap")
                break
        
        # Assertions
        self.assertGreater(clips_retrieved, 0, "Should retrieve clips from available folders")
        print(f"Retrieved {clips_retrieved} clips despite intermittent gaps")


if __name__ == '__main__':
    unittest.main() 