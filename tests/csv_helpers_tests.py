import unittest

from utils.csv_helpers import parse_change_string


class CsvHelpersTests(unittest.TestCase):
    def test_change_notation_parsing(self):
        cases = {
            'PB1-F2:V56A': ('PB1-F2', 'V', 56, 'A')
        }
        for k, v in cases.items():
            self.assertEqual(v, parse_change_string(k))  # add assertion here


if __name__ == '__main__':
    unittest.main()
