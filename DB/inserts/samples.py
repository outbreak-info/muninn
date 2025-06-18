from math import floor

import polars as pl
from sqlalchemy import select, insert

from DB.engine import get_async_session, get_asyncpg_connection
from DB.models import Sample
from utils.constants import ASYNCPG_MAX_QUERY_ARGS, StandardColumnNames, ConstraintNames
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


async def copy_insert_samples(samples: pl.DataFrame) -> str:
    columns = list(samples.columns)
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Sample.__tablename__,
        records=samples.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res

async def batch_upsert_samples(samples: pl.DataFrame):
    update_columns = [
        StandardColumnNames.ref_dp,
        StandardColumnNames.alt_dp,
        StandardColumnNames.alt_freq,
        StandardColumnNames.ref_rv,
        StandardColumnNames.alt_rv,
        StandardColumnNames.ref_qual,
        StandardColumnNames.alt_qual,
        StandardColumnNames.total_dp,
        StandardColumnNames.pval,
        StandardColumnNames.pass_qc,
    ]
    all_columns = update_columns + [
        StandardColumnNames.sample_id,
        StandardColumnNames.allele_id
    ]

    batch_size = floor(ASYNCPG_MAX_QUERY_ARGS / len(all_columns))
    slice_start = 0
    while slice_start < len(variants):
        variants_slice = variants.slice(slice_start, batch_size)
        slice_start += batch_size

        base_insert = (
            insert(Sample)
            .values(
                variants_slice
                .select([pl.col(cn) for cn in all_columns])
                .to_dicts()
            )
        )

        async with get_async_session() as session:
            await session.execute(
                base_insert.on_conflict_do_update(
                    constraint=ConstraintNames.,
                    set_={k: getattr(base_insert.excluded, k) for k in update_columns}
                )
            )
            await session.commit()