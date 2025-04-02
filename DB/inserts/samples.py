from sqlalchemy import select

from DB.engine import get_async_session
from utils.errors import NotFoundError
from DB.models import Sample


async def find_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_


async def find_or_insert_sample(s: Sample) -> (int, bool):
    preexisting = True
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == s.accession)
        )
        if id_ is None:
            preexisting = False
            session.add(s)
            await session.commit()
            await session.refresh(s)
            id_ = s.id
        return id_, preexisting
