from parser.parser import parser
from parser.tokenizer import lexer


def basic_test():
    inputs = [
        'foo = flu & bar = baz',
        '!foo = 1 | bar = 2.2',
        '!foo = 1 | (bar = 2.2 & baz = 4000)',
    ]

    for i in inputs:
        lexer.input(i)
        while True:
            tok = lexer.token()
            if not tok:
                break
            print(tok)


            res = parser.parse(i)
            print(res)


if __name__ == '__main__':
    basic_test()