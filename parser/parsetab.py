
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'leftANDORrightNOTAND DATE EQUALS GT GTE LPAREN LT LTE NOT NOT_EQUALS NUMBER OR RPAREN WORDexpression : expression AND expressionexpression : expression OR expressionexpression : NOT expressionexpression : termexpression : LPAREN expression RPARENterm : LPAREN term RPARENterm : field EQUALS valueterm : field NOT_EQUALS valueterm : field GT comparableterm : field LT comparableterm : field GTE comparableterm : field LTE comparablevalue : WORDfield : WORDvalue : comparablecomparable : NUMBERcomparable : DATE'
    
_lr_action_items = {'NOT':([0,2,4,7,8,],[2,2,2,2,2,]),'LPAREN':([0,2,4,7,8,],[4,4,4,4,4,]),'WORD':([0,2,4,7,8,12,13,],[6,6,6,6,6,23,23,]),'$end':([1,3,9,18,19,20,21,22,23,24,25,26,27,28,29,30,31,],[0,-4,-3,-1,-2,-5,-6,-7,-13,-15,-16,-17,-8,-9,-10,-11,-12,]),'AND':([1,3,9,10,11,18,19,20,21,22,23,24,25,26,27,28,29,30,31,],[7,-4,-3,7,-4,-1,-2,-5,-6,-7,-13,-15,-16,-17,-8,-9,-10,-11,-12,]),'OR':([1,3,9,10,11,18,19,20,21,22,23,24,25,26,27,28,29,30,31,],[8,-4,-3,8,-4,-1,-2,-5,-6,-7,-13,-15,-16,-17,-8,-9,-10,-11,-12,]),'RPAREN':([3,9,10,11,18,19,20,21,22,23,24,25,26,27,28,29,30,31,],[-4,-3,20,21,-1,-2,-5,-6,-7,-13,-15,-16,-17,-8,-9,-10,-11,-12,]),'EQUALS':([5,6,],[12,-14,]),'NOT_EQUALS':([5,6,],[13,-14,]),'GT':([5,6,],[14,-14,]),'LT':([5,6,],[15,-14,]),'GTE':([5,6,],[16,-14,]),'LTE':([5,6,],[17,-14,]),'NUMBER':([12,13,14,15,16,17,],[25,25,25,25,25,25,]),'DATE':([12,13,14,15,16,17,],[26,26,26,26,26,26,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression':([0,2,4,7,8,],[1,9,10,18,19,]),'term':([0,2,4,7,8,],[3,3,11,3,3,]),'field':([0,2,4,7,8,],[5,5,5,5,5,]),'value':([12,13,],[22,27,]),'comparable':([12,13,14,15,16,17,],[24,24,28,29,30,31,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> expression","S'",1,None,None,None),
  ('expression -> expression AND expression','expression',3,'p_expression_and_expression','parser.py',18),
  ('expression -> expression OR expression','expression',3,'p_expression_or_expression','parser.py',23),
  ('expression -> NOT expression','expression',2,'p_not_expression','parser.py',28),
  ('expression -> term','expression',1,'p_expression_term','parser.py',33),
  ('expression -> LPAREN expression RPAREN','expression',3,'p_paren_expression','parser.py',38),
  ('term -> LPAREN term RPAREN','term',3,'p_paren_term','parser.py',43),
  ('term -> field EQUALS value','term',3,'p_term_eq','parser.py',48),
  ('term -> field NOT_EQUALS value','term',3,'p_term_neq','parser.py',53),
  ('term -> field GT comparable','term',3,'p_term_gt','parser.py',58),
  ('term -> field LT comparable','term',3,'p_term_lt','parser.py',63),
  ('term -> field GTE comparable','term',3,'p_term_gte','parser.py',68),
  ('term -> field LTE comparable','term',3,'p_term_lte','parser.py',73),
  ('value -> WORD','value',1,'p_word_value','parser.py',78),
  ('field -> WORD','field',1,'p_word_field','parser.py',83),
  ('value -> comparable','value',1,'p_comparable_value','parser.py',88),
  ('comparable -> NUMBER','comparable',1,'p_number_comparable','parser.py',93),
  ('comparable -> DATE','comparable',1,'p_date_comparable','parser.py',98),
]
