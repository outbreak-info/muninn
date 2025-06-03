import polars as pl
from sqlalchemy import select

from DB.engine import get_async_session
from DB.models import Sample
from utils.errors import NotFoundError
from utils.gathering_task_group import GatheringTaskGroup


async def find_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_


async def find_or_insert_sample(s: Sample, upsert: bool = False) -> (int, bool):
    """
    :param s: Sample
    :param upsert: If true, use the provided sample's data to replace an existing sample with the same accession
    :return: (int: id of sample, bool: did a sample with this accession already exist?)
    """
    preexisting = True
    async with get_async_session() as session:
        existing: Sample = await session.scalar(
            select(Sample)
            .where(Sample.accession == s.accession)
        )
        if existing is None:
            preexisting = False
            session.add(s)
            await session.commit()
            await session.refresh(s)
            existing = s
        elif upsert:
            existing.copy_from(s)
            await session.commit()

        return existing.id, preexisting


# todo: fix this
async def _find_sample_id_shim(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    return id_


async def batch_find_samples(samples: pl.DataFrame, accession_name: str = 'accession') -> pl.DataFrame:
    async with GatheringTaskGroup() as tg:
        for row in samples.iter_rows(named=True):
            tg.create_task(
                    _find_sample_id_shim(row[accession_name])
            )
    samples = samples.with_columns(
        pl.Series('sample_id', tg.results())
    )
    return samples