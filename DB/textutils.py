import sqlalchemy
from sqlalchemy.sql.elements import TextClause


def clean_sql_text(messy: str) -> str:
    # split lines, skip blank lines
    lines =  [l for l in messy.split('\n') if l.strip() != '']

    min_leading_spaces = float('inf')
    for l in lines:
        leading_spaces = len(l) - len(l.lstrip(' '))
        min_leading_spaces = min(leading_spaces, min_leading_spaces)

    cleanlines = [l.removeprefix(' ' * min_leading_spaces) for l in lines]
    return '\n'.join(cleanlines)

def text(text_: str) -> TextClause:
    return sqlalchemy.text(clean_sql_text(text_))