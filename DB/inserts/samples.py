from math import floor

import polars as pl
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from DB.engine import get_async_write_session, get_asyncpg_connection, get_async_session, get_uri_for_polars
from DB.models import Sample
from utils.constants import ASYNCPG_MAX_QUERY_ARGS, StandardColumnNames, ConstraintNames
from utils.errors import NotFoundError


async def find_or_insert_sample(s: Sample, upsert: bool = False) -> (int, bool):
    """
    :param s: Sample
    :param upsert: If true, use the provided sample's data to replace an existing sample with the same accession
    :return: (int: id of sample, bool: did a sample with this accession already exist?)
    """
    preexisting = True
    async with get_async_write_session() as session:
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
        StandardColumnNames.bio_project,
        StandardColumnNames.bio_sample_model,
        StandardColumnNames.center_name,
        StandardColumnNames.experiment,
        StandardColumnNames.host,
        StandardColumnNames.instrument,
        StandardColumnNames.platform,
        StandardColumnNames.isolate,
        StandardColumnNames.library_name,
        StandardColumnNames.library_layout,
        StandardColumnNames.library_selection,
        StandardColumnNames.library_source,
        StandardColumnNames.organism,
        StandardColumnNames.is_retracted,
        StandardColumnNames.isolation_source,
        StandardColumnNames.collection_start_date,
        StandardColumnNames.collection_end_date,
        StandardColumnNames.release_date,
        StandardColumnNames.creation_date,
        StandardColumnNames.version,
        StandardColumnNames.sample_name,
        StandardColumnNames.sra_study,
        StandardColumnNames.geo_location_id,
        StandardColumnNames.consent_level,
        StandardColumnNames.assay_type,
        StandardColumnNames.bases,
        StandardColumnNames.bytes,
        StandardColumnNames.datastore_filetype,
        StandardColumnNames.datastore_region,
        StandardColumnNames.datastore_provider,
    ]
    # this is just the columns expected to be null in the SC2 data
    # if there are errors because another col is showing up as null, it may need to be added
    nullable_columns = [
        StandardColumnNames.bio_sample_accession,
        StandardColumnNames.retraction_detected_date,
        StandardColumnNames.serotype,
        StandardColumnNames.avg_spot_length,
    ]
    for colname in nullable_columns:
        if colname in samples.columns:
            update_columns.append(colname)

    all_columns = update_columns + [StandardColumnNames.accession]

    batch_size = floor(ASYNCPG_MAX_QUERY_ARGS / len(all_columns))
    slice_start = 0
    while slice_start < len(samples):
        updates_slice = samples.slice(slice_start, batch_size)
        slice_start += batch_size

        base_insert = (
            insert(Sample)
            .values(
                updates_slice
                .select([pl.col(cn) for cn in all_columns])
                .to_dicts()
            )
        )

        async with get_async_write_session() as session:
            await session.execute(
                base_insert.on_conflict_do_update(
                    constraint=ConstraintNames.uq_samples_accession,
                    set_={k: getattr(base_insert.excluded, k) for k in update_columns}
                )
            )
            await session.commit()


async def get_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_


async def get_samples_accession_and_id_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'select id, accession from samples;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.sample_id})
