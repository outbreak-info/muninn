import re

import ply.yacc as yacc

from DB.models import Sample
from .tokenizer import tokens

model = Sample

assert len(tokens) > 0

def p_term_and_expression(p):
    'expression : term AND expression'
    p[0] = f'{p[1]} AND {p[3]}'

def p_term_or_expression(p):
    'expression : term OR expression'
    p[0] = f'{p[1]} OR {p[3]}'

def p_expression_term(p):
    'expression : term'
    p[0] = p[1]

def p_term_eq(p):
    'term : field EQUALS value'
    p[0] = f"{p[1]} = {p[3]}"

def p_term_neq(p):
    'term : field NOT_EQUALS value'
    p[0] = f'{p[1]} <> {p[3]}'

def p_term_gt(p):
    'term : field GT comparable'
    p[0] = f'{p[1]} > {p[3]}'

def p_term_lt(p):
    'term : field LT comparable'
    p[0] = f'{p[1]} < {p[3]}'

def p_term_gte(p):
    'term : field GTE comparable'
    p[0] = f'{p[1]} >= {p[3]}'

def p_term_lte(p):
    'term : field LTE comparable'
    p[0] = f'{p[1]} <= {p[3]}'

def p_not_expression(p):
    'expression : NOT expression'
    p[0] = f'NOT {p[2]}'

def p_paren_expression(p):
    'expression : LPAREN expression RPAREN'
    p[0] = f'({p[2]})'


def p_word_value(p):
    'value : WORD'
    p[0] = f'\'{p[1]}\''

def p_word_field(p):
    'field : WORD'
    p[0] = p[1]

def p_comparable_value(p):
    'value : comparable'
    p[0] = p[1]

def p_number_comparable(p):
    'comparable : NUMBER'
    p[0] = p[1]

def p_date_comparable(p):
    'comparable : DATE'
    p[0] = f'\'{p[1]}\''


def p_error(p):
    raise ValueError(f'Syntax error in: {p}')

parser = yacc.yacc()
