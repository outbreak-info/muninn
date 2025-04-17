import unittest

from utils.geodata import parse_geo_loc


class GeoLocParsingTests(unittest.TestCase):

    def test_valid_inputs(self):
        cases = {
            'USA': ('usa', None, None),
            'USA:': ('usa', '', None),
            'USA: MD': ('usa', 'maryland', None),
            'USA: Maryland': ('usa', 'maryland', None),
            'USA: Silver Spring, MD': ('usa', 'maryland', 'silver spring'),
            'USA: Maryland, Montgomery County': ('usa', 'maryland', 'montgomery county'),
            'USA: Alaska, Matanuska-Susitna Borough': ('usa', 'alaska', 'matanuska-susitna borough'),
            'USA: Foo': ('usa', 'foo', None),
            'USA: Foo, Bar': ('usa', 'foo', 'bar'),
        }

        for input_, expected in cases.items():
            self.assertEqual(expected, parse_geo_loc(input_))


    def test_invalid_inputs(self):
        cases = [
            'Foo',
            'Foo:Bar:Baz',
        ]

        for input_ in cases:
            print(input_)
            self.assertRaises(ValueError, parse_geo_loc, input_)