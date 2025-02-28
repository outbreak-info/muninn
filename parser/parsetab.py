
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'AND EQUALS LPAREN NOT NUMBER OR RPAREN WORDexpression : expression AND expressionexpression : expression OR expressionexpression : expression EQUALS termexpression : NOT expressionexpression : WORDterm : WORDterm : NUMBERexpression : LPAREN expression RPAREN'
    
_lr_action_items = {'NOT':([0,2,4,5,6,],[2,2,2,2,2,]),'WORD':([0,2,4,5,6,7,],[3,3,3,3,3,13,]),'LPAREN':([0,2,4,5,6,],[4,4,4,4,4,]),'$end':([1,3,8,10,11,12,13,14,15,],[0,-5,-4,-1,-2,-3,-6,-7,-8,]),'AND':([1,3,8,9,10,11,12,13,14,15,],[5,-5,5,5,5,5,-3,-6,-7,-8,]),'OR':([1,3,8,9,10,11,12,13,14,15,],[6,-5,6,6,6,6,-3,-6,-7,-8,]),'EQUALS':([1,3,8,9,10,11,12,13,14,15,],[7,-5,7,7,7,7,-3,-6,-7,-8,]),'RPAREN':([3,8,9,10,11,12,13,14,15,],[-5,-4,15,-1,-2,-3,-6,-7,-8,]),'NUMBER':([7,],[14,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression':([0,2,4,5,6,],[1,8,9,10,11,]),'term':([7,],[12,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> expression","S'",1,None,None,None),
  ('expression -> expression AND expression','expression',3,'p_expression_and','parser.py',11),
  ('expression -> expression OR expression','expression',3,'p_expression_or','parser.py',16),
  ('expression -> expression EQUALS term','expression',3,'p_expression_eq','parser.py',20),
  ('expression -> NOT expression','expression',2,'p_expression_not','parser.py',24),
  ('expression -> WORD','expression',1,'p_expression_word','parser.py',28),
  ('term -> WORD','term',1,'p_term_word','parser.py',32),
  ('term -> NUMBER','term',1,'p_term_number','parser.py',36),
  ('expression -> LPAREN expression RPAREN','expression',3,'p_expression','parser.py',40),
]
