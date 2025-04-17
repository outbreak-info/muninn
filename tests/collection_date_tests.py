from datetime import date
import unittest

from utils.dates_and_times import parse_collection_start_and_end


class TestCollectionDateParsing(unittest.TestCase):

    def test_valid_inputs(self):
        cases = {
            '2024': (date(2024, 1, 1), date(2024, 12, 31)),
            '2025-4-16': (date(2025, 4, 16), date(2025, 4, 16)),
            '2025-4-16T': (date(2025, 4, 16), date(2025, 4, 16)),
            '2025-4-16/2025-5-17': (date(2025, 4, 16), date(2025, 5, 17)),
            '2025-4-16T15:40:34/2025-5-17': (date(2025, 4, 16), date(2025, 5, 17)),
            '2025-4-16/2025-5-17T15:40:34': (date(2025, 4, 16), date(2025, 5, 17)),
            '2025-4-16T15:40:34/2025-5-17T15:40:34': (date(2025, 4, 16), date(2025, 5, 17)),
            '1-1-1': (date(1, 1, 1), date(1, 1, 1)), # technically there was a year 1
        }

        for input_, expected in cases.items():
            self.assertEqual(expected, parse_collection_start_and_end(input_))


    def test_invalid_inputs(self):
        cases = [
            '2025-4-16/',
            '2025-4',
            '2025-0',
            '2025-1-0',
            '/2025-4-16'
        ]
        for input_ in cases:
            print(input_)
            self.assertRaises(ValueError, parse_collection_start_and_end, input_)