import unittest
from unittest.mock import patch, MagicMock
from orca_hls_utils.DateRangeHLSStream import DateRangeHLSStream
import datetime as dt
import pytz
import time

class TestDateRangeHLSStreamEdgeCases(unittest.TestCase):
    def setUp(self):
        import logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('DateRangeHLSStream')
        self.logger.setLevel(logging.WARNING)
        self.stream_base = 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_bush_point'
        self.polling_interval = 60
        self.unix_time_in_data = 1745564417	  # Epoch time for '2025-04-28 07:00'
        self.unix_time_out_data = int(dt.datetime(2025, 5, 5, 12, 0).timestamp())  # Convert '2025-05-05T12:00' to epoch time
        self.unix_time_outer_data = int(dt.datetime(2025, 5, 6, 12, 0).timestamp())  # Convert '2025-05-06T12:00' to epoch time
        self.wav_dir = '/tmp/wav_dir'

        print("\n in-data:", self.unix_time_in_data)
        print("out-data:", self.unix_time_out_data)
        print("outer-data:", self.unix_time_outer_data)

    def test_no_data_out_to_out(self):
        print("\n\nTesting no data from out to out...")
        stream = DateRangeHLSStream(self.stream_base, self.polling_interval, self.unix_time_out_data, self.unix_time_outer_data, self.wav_dir)
        result = stream.get_next_clip()
        self.assertEqual(result, (None, None, None))
        self.assertTrue(stream.is_end_of_stream)

    def test_no_data_in_to_out(self):
        print("\n\nTesting no data from in to out...")
        stream = DateRangeHLSStream(self.stream_base, self.polling_interval, self.unix_time_in_data, self.unix_time_out_data, self.wav_dir)
        # stream.current_folder_index = 1  # last folder
        result = stream.get_next_clip()
        self.assertNotEqual(result, (None, None, None))
        self.assertFalse(stream.is_end_of_stream)


if __name__ == '__main__':
    unittest.main()
