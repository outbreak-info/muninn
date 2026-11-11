import unittest

from utils.errors import ParsingError
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

    def test_dates_allowed(self):
        # these should pass without problems
        # no asserts needed, if there's an error the test fails
        dates = [
            '2025-4-2',
            '2025-04-02'
        ]
        for w in dates:
            lexer.input(w)
            while True:
                if not lexer.token():
                    break

    def test_wildcard_words(self):
        for w in ['XFG*', 'XFG.3*', 'B.1.1.7*', 'A*', 'foo_bar*']:
            with self.subTest(word=w):
                lexer.input(w)
                tok = lexer.token()
                self.assertEqual(('WILDCARD_WORD', w), (tok.type, tok.value))
                self.assertIsNone(lexer.token())

        # a value without '*' is still a plain WORD
        lexer.input('XFG.3')
        self.assertEqual('WORD', lexer.token().type)

    def test_wildcards_not_allowed(self):
        def tester(s):
            lexer.input(s)
            while lexer.token():
                pass

        for w in ['*', 'XFG**', 'foo-*', '*XFG', 'XFG *']:
            with self.subTest(word=w):
                self.assertRaises(ParsingError, tester, w)
