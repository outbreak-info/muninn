import unittest

from parser.ParsingError import ParsingError
from parser.tokenizer import lexer


class TestTokenizer(unittest.TestCase):

    def test_words_allowed(self):
        # these should pass without problems
        # no asserts needed, if there's an error the test fails
        words = [
            'A',
            'z',
            'FOO',
            'foo',
            'foo_bar',
            'foo-bar',
            'foo bar',
            'foo_',
            'foo7',
            'HA',
            'HA:cds-XAJ25415.1',
        ]
        for w in words:
            lexer.input(w)
            while True:
                if not lexer.token():
                    break

    def test_words_not_allowed(self):

        not_words = [
            '-012a',
            '8foo',
            '_foo',
            'foo-',
        ]

        def tester(s):
            lexer.input(s)
            while True:
                if not lexer.token():
                    break
            print(f'failed on: {s}')

        for w in not_words:
            self.assertRaises(ParsingError, tester, w)
