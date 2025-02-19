from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Sample, Mutation


def get_samples_via_mutation_by_allele_id(allele_id: int) -> List['Sample']:
    mutations_query = select(Mutation).where(Mutation.allele_id == allele_id).with_only_columns(Mutation.sample_id)
    samples_query = select(Sample).filter(Sample.id.in_(mutations_query))

    with Session(engine) as session:
        samples = session.execute(samples_query).scalars()
        return [s for s in samples]