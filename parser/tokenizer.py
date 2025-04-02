import dateutil.parser
import  ply.lex as lex

from utils.errors import ParsingError

tokens = (
    'EQUALS',
    'AND',
    'OR',
    'NOT',
    'LPAREN',
    'RPAREN',
    'WORD',
    'NUMBER',
    'NOT_EQUALS',
    'DATE',
    'GT',
    'LT',
    'GTE',
    'LTE',
)


t_EQUALS = r'='
t_AND = r'\^'
t_OR = r'\|'
t_NOT = r'!'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_WORD = r'\b[a-zA-Z]([\w\-:\. ]*[\w\-])?\b'
t_NOT_EQUALS = r'!='
t_GT = r'>'
t_LT = r'<'
t_GTE = r'>='
t_LTE = r'<='



def t_DATE(t):
    r'\d{4}-\d{1,2}-\d{1,2}'
    d = dateutil.parser.parse(t.value)
    t.value = d.date().isoformat()
    return t


def t_NUMBER(t):
    r'-?\d+(\.\d+)?'
    if '.' in t.value:
        t.value = float(t.value)
    else:
        t.value = int(t.value)
    return t

t_ignore = ' \t'


def t_error(t):
    raise ParsingError(f'Illegal character {t}')

lexer = lex.lex()