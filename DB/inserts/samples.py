from sqlalchemy import select

from DB.engine import get_async_session
from DB.errors import NotFoundError
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
