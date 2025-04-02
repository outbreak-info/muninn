from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import PhenotypeMetric
from api.models import PhenotypeMetricInfo


def get_all_pheno_metrics() -> List[PhenotypeMetricInfo]:
    with Session(engine) as session:
        res = session.execute(
            select(PhenotypeMetric)
        ).scalars()
        out_data = [PhenotypeMetricInfo.from_db_object(pm) for pm in res]
    return out_data