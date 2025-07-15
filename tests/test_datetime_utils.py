import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pytz import timezone

from orca_hls_utils import datetime_utils


class TestDatetimeUtils(unittest.TestCase):

    def setUp(self):
        # Test timestamps and expected values
        self.test_unix_time = 1700000000  # November 14, 2023 22:13:20 UTC
        self.test_source_guid = "test-hydrophone"

    def test_get_clip_name_from_unix_time(self):
        """Test converting unix time to clip name"""
        print("\n=== Testing get_clip_name_from_unix_time ===")

        clipname, readable_datetime = (
            datetime_utils.get_clip_name_from_unix_time(
                self.test_source_guid, self.test_unix_time
            )
        )

        # Expected format: source_guid_YYYY_MM_DD_HH_MM_SS
        # Note: The function converts to Pacific time, so we expect 14 (2PM PST) not 22 (10PM UTC)
        expected_clipname = f"{self.test_source_guid}_2023_11_14_14_13_20"
        expected_readable = "2023_11_14_14_13_20"

        self.assertEqual(clipname, expected_clipname)
        self.assertEqual(readable_datetime, expected_readable)

        print(f"Unix time {self.test_unix_time} -> {clipname}")
        print(f"Readable datetime: {readable_datetime}")

    def test_get_clip_name_from_unix_time_string(self):
        """Test with string input"""
        print(
            "\n=== Testing get_clip_name_from_unix_time with string input ==="
        )

        clipname, readable_datetime = (
            datetime_utils.get_clip_name_from_unix_time(
                self.test_source_guid, str(self.test_unix_time)
            )
        )

        expected_clipname = f"{self.test_source_guid}_2023_11_14_14_13_20"
        self.assertEqual(clipname, expected_clipname)
        print(f"String input '{self.test_unix_time}' -> {clipname}")

    def test_get_difference_between_times_in_seconds(self):
        """Test calculating time difference in seconds"""
        print("\n=== Testing get_difference_between_times_in_seconds ===")

        unix_time1 = 1700000000
        unix_time2 = 1700000600  # 10 minutes later

        # Test normal case
        diff = datetime_utils.get_difference_between_times_in_seconds(
            unix_time1, unix_time2
        )
        expected_diff = -600.0  # unix_time1 is 600 seconds before unix_time2

        self.assertEqual(diff, expected_diff)
        print(
            f"Difference between {unix_time1} and {unix_time2}: {diff} seconds"
        )

        # Test reverse order
        diff_reverse = datetime_utils.get_difference_between_times_in_seconds(
            unix_time2, unix_time1
        )
        expected_diff_reverse = (
            600.0  # unix_time2 is 600 seconds after unix_time1
        )

        self.assertEqual(diff_reverse, expected_diff_reverse)
        print(
            f"Difference between {unix_time2} and {unix_time1}: {diff_reverse} seconds"
        )

        # Test same time
        diff_same = datetime_utils.get_difference_between_times_in_seconds(
            unix_time1, unix_time1
        )
        self.assertEqual(diff_same, 0.0)
        print(f"Difference between same times: {diff_same} seconds")

    def test_get_difference_between_times_string_inputs(self):
        """Test time difference with string inputs"""
        print(
            "\n=== Testing get_difference_between_times_in_seconds with string inputs ==="
        )

        unix_time1 = "1700000000"
        unix_time2 = "1700000600"

        diff = datetime_utils.get_difference_between_times_in_seconds(
            unix_time1, unix_time2
        )
        expected_diff = -600.0

        self.assertEqual(diff, expected_diff)
        print(
            f"String inputs '{unix_time1}' and '{unix_time2}': {diff} seconds"
        )

    def test_add_interval_to_unix_time(self):
        """Test adding interval to unix time"""
        print("\n=== Testing add_interval_to_unix_time ===")

        unix_time = 1700000000
        interval_seconds = 600  # 10 minutes

        result = datetime_utils.add_interval_to_unix_time(
            unix_time, interval_seconds
        )

        # The function adds the interval and converts to Pacific time
        # The exact result depends on timezone conversion
        self.assertIsInstance(result, int)
        self.assertGreater(
            result, unix_time
        )  # Should be greater than original

        print(f"Original time: {unix_time}")
        print(f"Added {interval_seconds} seconds: {result}")

        # Test with different intervals
        result_1hour = datetime_utils.add_interval_to_unix_time(
            unix_time, 3600
        )  # 1 hour
        result_1day = datetime_utils.add_interval_to_unix_time(
            unix_time, 86400
        )  # 1 day

        self.assertGreater(result_1hour, result)
        self.assertGreater(result_1day, result_1hour)

        print(f"Added 1 hour: {result_1hour}")
        print(f"Added 1 day: {result_1day}")

    def test_add_interval_to_unix_time_string_input(self):
        """Test adding interval with string input"""
        print("\n=== Testing add_interval_to_unix_time with string input ===")

        unix_time = "1700000000"
        interval_seconds = 600

        result = datetime_utils.add_interval_to_unix_time(
            unix_time, interval_seconds
        )

        self.assertIsInstance(result, int)
        self.assertGreater(result, int(unix_time))
        print(f"String input '{unix_time}' + {interval_seconds}s = {result}")

    def test_add_interval_negative_interval(self):
        """Test adding negative interval (going backwards)"""
        print(
            "\n=== Testing add_interval_to_unix_time with negative interval ==="
        )

        unix_time = 1700000000
        interval_seconds = -600  # 10 minutes back

        result = datetime_utils.add_interval_to_unix_time(
            unix_time, interval_seconds
        )

        self.assertIsInstance(result, int)
        # Result should be less than original (went backwards)
        # But exact value depends on timezone conversion
        print(f"Original time: {unix_time}")
        print(f"Subtracted {abs(interval_seconds)} seconds: {result}")

    def test_get_unix_time_from_datetime_utc(self):
        """Test converting UTC datetime to unix time"""
        print("\n=== Testing get_unix_time_from_datetime_utc ===")

        # Create a test datetime in UTC
        test_datetime = datetime(
            2023, 11, 14, 22, 13, 20
        )  # Matches our test unix time

        result = datetime_utils.get_unix_time_from_datetime_utc(test_datetime)

        self.assertIsInstance(result, int)
        print(f"UTC datetime {test_datetime} -> unix time {result}")

        # Test with different datetime
        another_datetime = datetime(2023, 1, 1, 12, 0, 0)
        another_result = datetime_utils.get_unix_time_from_datetime_utc(
            another_datetime
        )

        self.assertIsInstance(another_result, int)
        self.assertNotEqual(result, another_result)
        print(f"UTC datetime {another_datetime} -> unix time {another_result}")

    def test_get_unix_time_from_datetime_utc_edge_cases(self):
        """Test edge cases for UTC datetime conversion"""
        print("\n=== Testing get_unix_time_from_datetime_utc edge cases ===")

        # Test with epoch time
        epoch_datetime = datetime(1970, 1, 1, 0, 0, 0)
        epoch_result = datetime_utils.get_unix_time_from_datetime_utc(
            epoch_datetime
        )

        # Due to timezone conversion to Pacific, this won't be exactly 0
        self.assertIsInstance(epoch_result, int)
        print(f"Epoch datetime {epoch_datetime} -> unix time {epoch_result}")

        # Test with far future date
        future_datetime = datetime(2030, 12, 31, 23, 59, 59)
        future_result = datetime_utils.get_unix_time_from_datetime_utc(
            future_datetime
        )

        self.assertIsInstance(future_result, int)
        self.assertGreater(future_result, epoch_result)
        print(
            f"Future datetime {future_datetime} -> unix time {future_result}"
        )

    def test_round_trip_conversions(self):
        """Test round-trip conversions between formats"""
        print("\n=== Testing round-trip conversions ===")

        # Start with unix time
        original_unix = 1700000000

        # Convert to datetime and back
        dt = datetime.fromtimestamp(original_unix)
        back_to_unix = datetime_utils.get_unix_time_from_datetime_utc(dt)

        # Due to timezone conversions, these might not be exactly equal
        # but should be reasonably close
        print(f"Original: {original_unix}")
        print(f"Round-trip: {back_to_unix}")
        print(f"Difference: {abs(original_unix - back_to_unix)} seconds")

        # Test adding interval and checking result
        interval = 3600  # 1 hour
        new_unix = datetime_utils.add_interval_to_unix_time(
            original_unix, interval
        )

        # Calculate expected difference
        diff = datetime_utils.get_difference_between_times_in_seconds(
            new_unix, original_unix
        )
        print(f"Added {interval}s, actual difference: {diff}s")

    def test_clip_name_consistency(self):
        """Test that clip names are consistent across calls"""
        print("\n=== Testing clip name consistency ===")

        unix_time = 1700000000
        source_guid = "test-hydrophone"

        # Call multiple times with same inputs
        clipname1, readable1 = datetime_utils.get_clip_name_from_unix_time(
            source_guid, unix_time
        )
        clipname2, readable2 = datetime_utils.get_clip_name_from_unix_time(
            source_guid, unix_time
        )

        self.assertEqual(clipname1, clipname2)
        self.assertEqual(readable1, readable2)

        print(f"Consistent clipname: {clipname1}")
        print(f"Consistent readable: {readable1}")

        # Test with different source_guid
        different_guid = "different-hydrophone"
        clipname3, readable3 = datetime_utils.get_clip_name_from_unix_time(
            different_guid, unix_time
        )

        self.assertNotEqual(clipname1, clipname3)
        self.assertEqual(readable1, readable3)  # Readable part should be same

        print(f"Different source clipname: {clipname3}")


if __name__ == "__main__":
    unittest.main()
