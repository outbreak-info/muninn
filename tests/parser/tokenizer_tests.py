from parser.parser import parser
from parser.tokenizer import lexer


def basic_test():
    inputs = [
        'host = cat',
        'host != domestic cat',
        'host = cat & accession = SRR28752446',
        'collection_date = 1970-01-02',
    ]

    for i in inputs:
        print(f'input: {i}')
        lexer.input(i)
        while True:
            tok = lexer.token()
            if not tok:
                break
            print(tok)

        res = parser.parse(i)
        print(res)

        print('-' * 10)


if __name__ == '__main__':
    basic_test()
