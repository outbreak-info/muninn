import  ply.lex as lex
tokens = (
    'EQUALS',
    'AND',
    'OR',
    'NOT',
    'LPAREN',
    'RPAREN',
    'WORD',
    'NUMBER'
)


t_EQUALS = r'='
t_AND = r'&'
t_OR = r'\|'
t_NOT = r'!'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_WORD = r'\w+'

def t_NUMBER(t):
    r'\d+(\.\d+)?'
    if '.' in t.value:
        t.value = float(t.value)
    else:
        t.value = int(t.value)
    return t

t_ignore = ' \t'

def t_error(t):
    print(f'Illegal character {t}')
    t.lexer.skip(1)

lexer = lex.lex()