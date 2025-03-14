import unittest

from parser.ParsingError import ParsingError
from parser.parser import parser


class TestParser(unittest.TestCase):

    def test_equal_term(self):
        res = parser.parse('host = cat')
        self.assertEqual('host = \'cat\'', res)

        res = parser.parse('host=cat')
        self.assertEqual('host = \'cat\'', res)

        self.assertRaises(ParsingError, parser.parse, 'host == cat')

    def test_not_equal_term(self):
        res = parser.parse('host != cat')
        self.assertEqual('host <> \'cat\'', res)

    def test_value_contains_space(self):
        res = parser.parse('host != domestic cat')
        self.assertEqual('host <> \'domestic cat\'', res)

    def test_and_expression(self):
        res = parser.parse('host = cat & accession = SRR28752446')
        self.assertEqual('host = \'cat\' AND accession = \'SRR28752446\'', res)

        res = parser.parse('host = cat & host = dog & host = bat & host = foo')
        self.assertEqual('host = \'cat\' AND host = \'dog\' AND host = \'bat\' AND host = \'foo\'', res)

        res = parser.parse('(host = cat) & accession = SRR28752446')
        self.assertEqual('(host = \'cat\') AND accession = \'SRR28752446\'', res)

    def test_or_expression(self):
        res = parser.parse('host = cat | accession != SRR28752446')
        self.assertEqual('host = \'cat\' OR accession <> \'SRR28752446\'', res)

    def test_not_expression(self):
        res = parser.parse('! host = cat')
        self.assertEqual('NOT host = \'cat\'', res)

    def test_paren_expression(self):
        res = parser.parse('!(host=cat & accession=SRR28752446)')
        self.assertEqual('NOT (host = \'cat\' AND accession = \'SRR28752446\')', res)

        res = parser.parse('(host=cat & accession=SRR28752446) | host = bird')
        self.assertEqual('(host = \'cat\' AND accession = \'SRR28752446\') OR host = \'bird\'', res)

        res = parser.parse('(host=cat & accession=SRR28752446) & host = bird')
        self.assertEqual('(host = \'cat\' AND accession = \'SRR28752446\') AND host = \'bird\'', res)

    def test_paren_term(self):
        res = parser.parse('(host = cat) & accession = SRR28752446')
        self.assertEqual('(host = \'cat\') AND accession = \'SRR28752446\'', res)

    def test_date_value(self):
        res = parser.parse('collection_date = 1970-01-02')
        self.assertEqual('collection_date = \'1970-01-02\'', res)

    def test_number_value(self):
        res = parser.parse('number = 42.17')
        self.assertEqual('number = 42.17', res)

        res = parser.parse('number = -17')
        self.assertEqual('number = -17', res)

        self.assertRaises(ParsingError, parser.parse, 'number = 42.')

        self.assertRaises(ParsingError, parser.parse, 'number = .42')

    def test_gt_term(self):
        res = parser.parse('number > 42.17')
        self.assertEqual('number > 42.17', res)

        res = parser.parse('date > 2017-10-31')
        self.assertEqual('date > \'2017-10-31\'', res)

    def test_lt_term(self):
        res = parser.parse('number < 42.17')
        self.assertEqual('number < 42.17', res)

        res = parser.parse('date < 2017-10-31')
        self.assertEqual('date < \'2017-10-31\'', res)

    def test_gte_term(self):
        res = parser.parse('number >= 42.17')
        self.assertEqual('number >= 42.17', res)

        res = parser.parse('date>=2017-10-31')
        self.assertEqual('date >= \'2017-10-31\'', res)

    def test_lte_term(self):
        res = parser.parse('number <= 42.17')
        self.assertEqual('number <= 42.17', res)

        res = parser.parse('date <= 2017-10-31')
        self.assertEqual('date <= \'2017-10-31\'', res)

    def test_compare_not_comparable(self):
        self.assertRaises(ParsingError, parser.parse, 'host > cat')
        self.assertRaises(ParsingError, parser.parse, 'host < domestic cat')
        self.assertRaises(ParsingError, parser.parse, 'host >= domestic cat')
        self.assertRaises(ParsingError, parser.parse, 'host <= cat')

    def test_quotes_not_allowed(self):
        self.assertRaises(ParsingError, parser.parse, 'host = \'cat\'')
        self.assertRaises(ParsingError, parser.parse, 'host = \"cat\"')

    def test_semicolon_not_allowed(self):
        self.assertRaises(ParsingError, parser.parse, 'host = cat;')

    def test_mismatched_parens(self):
        self.assertRaises(ParsingError, parser.parse, '(host = cat')
        self.assertRaises(ParsingError, parser.parse, 'host = cat)')

    def test_paren_field_not_allowed(self):
        self.assertRaises(ParsingError, parser.parse, '(host) = cat')

    def test_paren_value_not_allowed(self):
        self.assertRaises(ParsingError, parser.parse, 'host = (cat)')

    def test_paren_bisect_term_not_allowed(self):
        self.assertRaises(ParsingError, parser.parse, 'host = (cat | host) = dog')