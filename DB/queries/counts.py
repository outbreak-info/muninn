from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import count

from DB.engine import engine
from DB.models import Sample


def count_samples_by_column(by_col: str):

    # todo: needs validation
    query = f'select {by_col}, count(*) from samples group by {by_col}'

    with Session(engine) as session:
        res = session.execute(text(query)).all()
        print(res)
        return res