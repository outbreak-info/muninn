
# parsetab.py
# This file is automatically generated. Do not edit.
# pylint: disable=W,C,R
_tabversion = '3.10'

_lr_method = 'LALR'

_lr_signature = 'AND DATE EQUALS GT GTE LPAREN LT LTE NOT NOT_EQUALS NUMBER OR RPAREN WORDexpression : term AND expressionexpression : term OR expressionexpression : termterm : field EQUALS valueterm : field NOT_EQUALS valueterm : field GT comparableterm : field LT comparableterm : field GTE comparableterm : field LTE comparableexpression : NOT expressionexpression : LPAREN expression RPARENvalue : WORDfield : WORDvalue : comparablecomparable : NUMBERcomparable : DATE'
    
_lr_action_items = {'NOT':([0,3,4,7,8,],[3,3,3,3,3,]),'LPAREN':([0,3,4,7,8,],[4,4,4,4,4,]),'WORD':([0,3,4,7,8,11,12,],[6,6,6,6,6,21,21,]),'$end':([1,2,9,17,18,19,20,21,22,23,24,25,26,27,28,29,],[0,-3,-10,-1,-2,-11,-4,-12,-14,-15,-16,-5,-6,-7,-8,-9,]),'AND':([2,20,21,22,23,24,25,26,27,28,29,],[7,-4,-12,-14,-15,-16,-5,-6,-7,-8,-9,]),'OR':([2,20,21,22,23,24,25,26,27,28,29,],[8,-4,-12,-14,-15,-16,-5,-6,-7,-8,-9,]),'RPAREN':([2,9,10,17,18,19,20,21,22,23,24,25,26,27,28,29,],[-3,-10,19,-1,-2,-11,-4,-12,-14,-15,-16,-5,-6,-7,-8,-9,]),'EQUALS':([5,6,],[11,-13,]),'NOT_EQUALS':([5,6,],[12,-13,]),'GT':([5,6,],[13,-13,]),'LT':([5,6,],[14,-13,]),'GTE':([5,6,],[15,-13,]),'LTE':([5,6,],[16,-13,]),'NUMBER':([11,12,13,14,15,16,],[23,23,23,23,23,23,]),'DATE':([11,12,13,14,15,16,],[24,24,24,24,24,24,]),}

_lr_action = {}
for _k, _v in _lr_action_items.items():
   for _x,_y in zip(_v[0],_v[1]):
      if not _x in _lr_action:  _lr_action[_x] = {}
      _lr_action[_x][_k] = _y
del _lr_action_items

_lr_goto_items = {'expression':([0,3,4,7,8,],[1,9,10,17,18,]),'term':([0,3,4,7,8,],[2,2,2,2,2,]),'field':([0,3,4,7,8,],[5,5,5,5,5,]),'value':([11,12,],[20,25,]),'comparable':([11,12,13,14,15,16,],[22,22,26,27,28,29,]),}

_lr_goto = {}
for _k, _v in _lr_goto_items.items():
   for _x, _y in zip(_v[0], _v[1]):
       if not _x in _lr_goto: _lr_goto[_x] = {}
       _lr_goto[_x][_k] = _y
del _lr_goto_items
_lr_productions = [
  ("S' -> expression","S'",1,None,None,None),
  ('expression -> term AND expression','expression',3,'p_term_and_expression','parser.py',13),
  ('expression -> term OR expression','expression',3,'p_term_or_expression','parser.py',17),
  ('expression -> term','expression',1,'p_expression_term','parser.py',21),
  ('term -> field EQUALS value','term',3,'p_term_eq','parser.py',25),
  ('term -> field NOT_EQUALS value','term',3,'p_term_neq','parser.py',29),
  ('term -> field GT comparable','term',3,'p_term_gt','parser.py',33),
  ('term -> field LT comparable','term',3,'p_term_lt','parser.py',37),
  ('term -> field GTE comparable','term',3,'p_term_gte','parser.py',41),
  ('term -> field LTE comparable','term',3,'p_term_lte','parser.py',45),
  ('expression -> NOT expression','expression',2,'p_not_expression','parser.py',49),
  ('expression -> LPAREN expression RPAREN','expression',3,'p_paren_expression','parser.py',53),
  ('value -> WORD','value',1,'p_word_value','parser.py',58),
  ('field -> WORD','field',1,'p_word_field','parser.py',62),
  ('value -> comparable','value',1,'p_comparable_value','parser.py',66),
  ('comparable -> NUMBER','comparable',1,'p_number_comparable','parser.py',70),
  ('comparable -> DATE','comparable',1,'p_date_comparable','parser.py',74),
]
