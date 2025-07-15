import unittest
from unittest.mock import patch, MagicMock, call
import boto3
from moto import mock_aws

from orca_hls_utils import s3_utils


class TestS3Utils(unittest.TestCase):
    
    def setUp(self):
        self.bucket_name = "test-bucket"
        self.prefix = "test-prefix/"
        
    @mock_aws
    def test_get_all_folders(self):
        """Test getting all folders from S3"""
        print("\n=== Testing get_all_folders ===")
        
        # Create mock S3 bucket and objects
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=self.bucket_name)
        
        # Create test objects that represent folders
        test_folders = [
            "test-prefix/1700000000/",
            "test-prefix/1700000600/",
            "test-prefix/1700001200/",
            "test-prefix/1700001800/",
        ]
        
        for folder in test_folders:
            s3_client.put_object(Bucket=self.bucket_name, Key=folder + "live.m3u8", Body=b"test")
            
        # Test the function
        result = s3_utils.get_all_folders(self.bucket_name, self.prefix)
        
        # Verify results
        self.assertEqual(len(result), 4)
        expected_folders = ["1700000000", "1700000600", "1700001200", "1700001800"]
        self.assertEqual(sorted(result), sorted(expected_folders))
        print(f"Found folders: {result}")
        
    def test_get_folders_between_timestamp(self):
        """Test filtering folders by timestamp range"""
        print("\n=== Testing get_folders_between_timestamp ===")
        
        # Test data
        bucket_list = ["1700000000", "1700000600", "1700001200", "1700001800", "1700002400"]
        start_time = "1700000600"
        end_time = "1700001800"
        
        # Test normal case
        result = s3_utils.get_folders_between_timestamp(bucket_list, start_time, end_time)
        expected = ["1700000600", "1700001200", "1700001800"]
        self.assertEqual(result, expected)
        print(f"Folders in range {start_time} to {end_time}: {result}")
        
        # Test edge cases
        # No folders in range
        result_empty = s3_utils.get_folders_between_timestamp(
            bucket_list, "1600000000", "1600000001"
        )
        self.assertEqual(result_empty, [])
        
        # Start time after all folders
        result_after = s3_utils.get_folders_between_timestamp(
            bucket_list, "1800000000", "1800000001"
        )
        self.assertEqual(result_after, [])
        
        # End time before all folders
        result_before = s3_utils.get_folders_between_timestamp(
            bucket_list, "1600000000", "1600000001"
        )
        self.assertEqual(result_before, [])
        
        print("Edge cases passed")
        
    def test_get_folders_between_timestamp_empty_list(self):
        """Test with empty bucket list"""
        print("\n=== Testing get_folders_between_timestamp with empty list ===")
        
        result = s3_utils.get_folders_between_timestamp([], "1700000000", "1700001800")
        self.assertEqual(result, [])
        
    @mock_aws
    def test_list_s3_files(self):
        """Test listing S3 files"""
        print("\n=== Testing list_s3_files ===")
        
        # Create mock S3 bucket and objects
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=self.bucket_name)
        
        # Create test files
        test_files = [
            "test-prefix/1700000000/live.m3u8",
            "test-prefix/1700000000/live000.ts",
            "test-prefix/1700000000/live001.ts",
            "test-prefix/1700000000/live002.ts",
        ]
        
        for file_key in test_files:
            s3_client.put_object(Bucket=self.bucket_name, Key=file_key, Body=b"test")
            
        # Test the function
        result = s3_utils.list_s3_files(self.bucket_name, self.prefix)
        
        # Verify results
        self.assertEqual(len(result), 4)
        self.assertEqual(sorted(result), sorted(test_files))
        print(f"Found files: {result}")
        
    @mock_aws
    def test_download_s3_object(self):
        """Test downloading S3 object"""
        print("\n=== Testing download_s3_object ===")
        
        # Create mock S3 bucket and object
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=self.bucket_name)
        
        test_key = "test-prefix/test-file.txt"
        test_content = b"test content"
        s3_client.put_object(Bucket=self.bucket_name, Key=test_key, Body=test_content)
        
        # Test the function
        import tempfile
        with tempfile.NamedTemporaryFile() as tmp_file:
            s3_utils.download_s3_object(self.bucket_name, test_key, tmp_file.name)
            
            # Verify content
            with open(tmp_file.name, 'rb') as f:
                downloaded_content = f.read()
            self.assertEqual(downloaded_content, test_content)
            print(f"Successfully downloaded file with content: {downloaded_content}")
            
    @patch('orca_hls_utils.s3_utils.s3fs.S3FileSystem')
    @patch('orca_hls_utils.s3_utils.m3u8.loads')
    @patch('orca_hls_utils.s3_utils.list_s3_files')
    def test_verify_playlist_matching_files(self, mock_list_s3_files, mock_m3u8_loads, mock_s3fs):
        """Test verify_playlist with matching files"""
        print("\n=== Testing verify_playlist with matching files ===")
        
        # Setup mocks
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance
        
        mock_file = MagicMock()
        mock_file.read.return_value = "mock m3u8 content"
        mock_s3_instance.open.return_value.__enter__.return_value = mock_file
        
        # Mock playlist with segments
        mock_playlist = MagicMock()
        mock_segment1 = MagicMock()
        mock_segment1.uri = "live001.ts"
        mock_segment2 = MagicMock()
        mock_segment2.uri = "live002.ts"
        mock_playlist.segments = [mock_segment1, mock_segment2]
        mock_m3u8_loads.return_value = mock_playlist
        
        # Mock S3 files
        mock_list_s3_files.return_value = [
            "test-prefix/1700000000/live.m3u8",
            "test-prefix/1700000000/live001.ts",
            "test-prefix/1700000000/live002.ts",
        ]
        
        # Test the function
        result = s3_utils.verify_playlist(self.bucket_name, "test-prefix/1700000000/")
        
        # Verify results
        self.assertIsNotNone(result)
        assert result is not None  # Type hint for linter
        self.assertEqual(result['missing_files'], [])
        self.assertEqual(result['extra_files'], [])
        self.assertEqual(result['length'], 2)
        print(f"Playlist verification result: {result}")
        
    @patch('orca_hls_utils.s3_utils.s3fs.S3FileSystem')
    @patch('orca_hls_utils.s3_utils.m3u8.loads')
    @patch('orca_hls_utils.s3_utils.list_s3_files')
    def test_verify_playlist_missing_files(self, mock_list_s3_files, mock_m3u8_loads, mock_s3fs):
        """Test verify_playlist with missing files"""
        print("\n=== Testing verify_playlist with missing files ===")
        
        # Setup mocks
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance
        
        mock_file = MagicMock()
        mock_file.read.return_value = "mock m3u8 content"
        mock_s3_instance.open.return_value.__enter__.return_value = mock_file
        
        # Mock playlist with segments
        mock_playlist = MagicMock()
        mock_segment1 = MagicMock()
        mock_segment1.uri = "live001.ts"
        mock_segment2 = MagicMock()
        mock_segment2.uri = "live002.ts"
        mock_segment3 = MagicMock()
        mock_segment3.uri = "live003.ts"
        mock_playlist.segments = [mock_segment1, mock_segment2, mock_segment3]
        mock_m3u8_loads.return_value = mock_playlist
        
        # Mock S3 files (missing live003.ts)
        mock_list_s3_files.return_value = [
            "test-prefix/1700000000/live.m3u8",
            "test-prefix/1700000000/live001.ts",
            "test-prefix/1700000000/live002.ts",
        ]
        
        # Test the function
        result = s3_utils.verify_playlist(self.bucket_name, "test-prefix/1700000000/")
        
        # Verify results
        self.assertIsNotNone(result)
        assert result is not None  # Type hint for linter
        self.assertEqual(result['missing_files'], ['003'])
        self.assertEqual(result['extra_files'], [])
        self.assertEqual(result['length'], 3)
        print(f"Playlist verification result: {result}")
        
    @patch('orca_hls_utils.s3_utils.s3fs.S3FileSystem')
    @patch('orca_hls_utils.s3_utils.m3u8.loads')
    def test_verify_playlist_load_error(self, mock_m3u8_loads, mock_s3fs):
        """Test verify_playlist when m3u8 loading fails"""
        print("\n=== Testing verify_playlist with load error ===")
        
        # Setup mocks to raise exception
        mock_s3_instance = MagicMock()
        mock_s3fs.return_value = mock_s3_instance
        mock_s3_instance.open.side_effect = Exception("S3 error")
        
        # Test the function
        result = s3_utils.verify_playlist(self.bucket_name, "test-prefix/1700000000/")
        
        # Should return None when there's an error
        self.assertIsNone(result)
        print("Error handling test passed")


if __name__ == '__main__':
    unittest.main() 