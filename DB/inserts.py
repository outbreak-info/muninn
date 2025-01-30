from typing import List, Dict

from sqlalchemy import insert
from sqlalchemy.orm import Session

from DB.engine import create_pg_engine
from DB.models import Metadata


def insert_metadata(data: List[Dict]) -> Dict:
    engine = create_pg_engine()
    with Session(engine) as session:
        res = session.scalars(
            insert(Metadata).returning(Metadata.id, sort_by_parameter_order=True),
            data
        )
        session.commit()
        ids = res.all()

    sra_to_pg_id = dict(zip([md['run'] for md in data], ids))
    return sra_to_pg_id


def insert_mutations(data: List[Dict], sra_to_pg_id: Dict):
    engine = create_pg_engine()
    raise NotImplementedError
