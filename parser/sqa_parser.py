import ply.yacc as yacc
from sqlalchemy import and_, or_, not_
from sqlalchemy.sql.operators import eq

from DB.models import Sample
from .tokenizer import tokens

model = Sample

assert len(tokens) > 0

def p_expression_and(p):
    'expression : expression AND expression'
    p[0] = and_(p[1], p[3])


def p_expression_or(p):
    'expression : expression OR expression'
    p[0] = or_(p[1], p[3])

def p_expression_eq(p):
    'expression : expression EQUALS term'
    p[0] = model.p[1] == p[3]

def p_expression_not(p):
    'expression : NOT expression'
    p[0] = f'(NOT ({p[2]}))'

def p_expression_word(p):
    'expression : WORD'
    p[0] = p[1]

def p_term_word(p):
    'term : WORD'
    p[0] = f"'{p[1]}'"

def p_term_number(p):
    'term : NUMBER'
    p[0] = p[1]

def p_error(p):
    print(f'Syntax error in: {p}')

parser = yacc.yacc()