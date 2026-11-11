import unittest

from utils.errors import ParsingError
from DB.queries.lineage_helpers import get_lineage_filter_sql
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
        res = parser.parse('host = cat ^ accession = SRR28752446')
        self.assertEqual('host = \'cat\' AND accession = \'SRR28752446\'', res)

        res = parser.parse('host = cat ^ host = dog ^ host = bat ^ host = foo')
        self.assertEqual('host = \'cat\' AND host = \'dog\' AND host = \'bat\' AND host = \'foo\'', res)

        res = parser.parse('(host = cat) ^ accession = SRR28752446')
        self.assertEqual('(host = \'cat\') AND accession = \'SRR28752446\'', res)

    def test_or_expression(self):
        res = parser.parse('host = cat | accession != SRR28752446')
        self.assertEqual('host = \'cat\' OR accession <> \'SRR28752446\'', res)

    def test_not_expression(self):
        res = parser.parse('! host = cat')
        self.assertEqual('NOT host = \'cat\'', res)

    def test_paren_expression(self):
        res = parser.parse('!(host=cat ^ accession=SRR28752446)')
        self.assertEqual('NOT (host = \'cat\' AND accession = \'SRR28752446\')', res)

        res = parser.parse('(host=cat ^ accession=SRR28752446) | host = bird')
        self.assertEqual('(host = \'cat\' AND accession = \'SRR28752446\') OR host = \'bird\'', res)

        res = parser.parse('(host=cat ^ accession=SRR28752446) ^ host = bird')
        self.assertEqual('(host = \'cat\' AND accession = \'SRR28752446\') AND host = \'bird\'', res)

        res = parser.parse('!(is_redacted)')
        self.assertEqual('NOT (is_redacted)', res)

        res = parser.parse('(!is_redacted)')
        self.assertEqual('(NOT is_redacted)', res)

    def test_paren_term(self):
        res = parser.parse('(host = cat) ^ accession = SRR28752446')
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

    def test_complex_not_expression(self):
        res = parser.parse('!(host = cat)')
        self.assertEqual('NOT (host = \'cat\')', res)

        res = parser.parse('number = 5 ^ !(host = cat)')
        self.assertEqual('number = 5 AND NOT (host = \'cat\')', res)

        res = parser.parse('number = 5 ^ !!(host = cat)')
        self.assertEqual('number = 5 AND NOT NOT (host = \'cat\')', res)

    def test_boolean_field(self):
        res = parser.parse('is_ww_sample')
        self.assertEqual('is_ww_sample', res)

        res = parser.parse('!is_redacted')
        self.assertEqual('NOT is_redacted', res)

    def test_complex_expression_with_boolean(self):
        res = parser.parse('number = 5 ^ !(host = cat) | is_ww_sample')
        self.assertEqual('number = 5 AND NOT (host = \'cat\') OR is_ww_sample', res)

        res = parser.parse('number = 5 ^ !!(host = cat) ^ (is_ww_sample)')
        self.assertEqual('number = 5 AND NOT NOT (host = \'cat\') AND (is_ww_sample)', res)

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

    def test_ampersand_banned(self):
        self.assertRaises(ParsingError, parser.parse, 'host = cat & foo != bar')

    def test_field_contains_space(self):
        self.assertRaises(ParsingError, parser.parse, 'host type = cat')

    def test_multiple_ands_ors(self):
        self.assertRaises(ParsingError, parser.parse, 'host = cat ^^ number = 5')
        self.assertRaises(ParsingError, parser.parse, 'host = cat || number = 5')
        self.assertRaises(ParsingError, parser.parse, 'host = cat |^ number = 5')

    def test_partial_terms(self):
        self.assertRaises(ParsingError, parser.parse, 'is_ww_sample = ')
        self.assertRaises(ParsingError, parser.parse, 'is_ww_sample=')
        self.assertRaises(ParsingError, parser.parse, '!is_ww_sample=')
        self.assertRaises(ParsingError, parser.parse, '=is_ww_sample')

    def test_lineage_wildcard(self):
        res = parser.parse('lineage_name = XFG*')
        self.assertEqual(res, get_lineage_filter_sql('XFG*'))
        self.assertTrue(res.startswith('((lineage_name, lineage_system_name) in ('))
        self.assertIn("l.lineage_name = 'XFG'", res)
        self.assertIn('lineages_deep_children', res)
        self.assertNotIn('*', res)

        res = parser.parse('lineage_name != B.1.1.7*')
        self.assertEqual(res, get_lineage_filter_sql('B.1.1.7*', negate=True))
        self.assertTrue(res.startswith('((lineage_name, lineage_system_name) not in ('))

    def test_lineage_wildcard_composes(self):
        wild = get_lineage_filter_sql('XFG*')
        res = parser.parse('lineage_name = XFG* ^ lineage_system_name = PANGO')
        self.assertEqual(f"{wild} AND lineage_system_name = 'PANGO'", res)

        res = parser.parse('host = cat ^ !(lineage_name = XFG* | lineage_name = BA.2)')
        self.assertEqual(f"host = 'cat' AND NOT ({wild} OR lineage_name = 'BA.2')", res)

    def test_wildcard_only_on_lineage_name(self):
        self.assertRaises(ParsingError, parser.parse, 'host = cat*')
        self.assertRaises(ParsingError, parser.parse, 'lineage_system_name = PANGO*')

    def test_bad_wildcards(self):
        for f in [
            'lineage_name = *',
            'lineage_name = XFG**',
            'lineage_name = X*G',
            'lineage_name = XFG *',
            'lineage_name = foo-*',
            'lineage_name* = XFG',
            'lineage_name > XFG*',
            'lineage_name <= XFG*',
            "lineage_name = XFG'*",
        ]:
            with self.subTest(filter=f):
                self.assertRaises(ParsingError, parser.parse, f)
