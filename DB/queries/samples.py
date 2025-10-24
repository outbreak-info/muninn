from typing import List, Dict

import polars as pl
from sqlalchemy import select, text, cast, Date, func, Integer
from sqlalchemy.orm import contains_eager
import dask.dataframe as dd

import DB.engine
from DB.engine import get_uri_for_polars, get_async_session, get_uri_for_dask
from DB.models import Sample, Mutation, GeoLocation, Allele, AminoAcid, IntraHostVariant, Translation
from api.models import SampleInfo
from parser.parser import parser
from utils.constants import StandardColumnNames, TableNames
from utils.errors import NotFoundError
from utils.constants import DateBinOpt


async def get_sample_by_id(sample_id: int) -> SampleInfo | None:
    query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(Sample.id == sample_id)
    )

    async with get_async_session() as session:
        result = await session.scalar(query)
    if result is None:
        return None
    return SampleInfo.from_db_object(result)


async def get_samples(query: str) -> List['SampleInfo']:
    user_defined_query = parser.parse(query)

    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(text(user_defined_query))
    )

    async with get_async_session() as session:
        samples = await session.scalars(samples_query)
        out_data = [SampleInfo.from_db_object(s) for s in samples]
    return out_data


async def get_samples_by_mutation(query: str) -> List['SampleInfo']:
    user_defined_query = parser.parse(query)

    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(
            Sample.id.in_(
                select(Mutation.sample_id)
                .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
                .join(Translation, Translation.id == Mutation.translation_id, isouter=True)
                .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
                .where(text(user_defined_query))

            )
        )
    )

    async with get_async_session() as session:
        samples = await session.scalars(samples_query)
        out_data = []
        for s in samples.unique():
            out_data.append(
                SampleInfo.from_db_object(s)
            )
    return out_data


async def get_samples_by_variant(query: str) -> List['SampleInfo']:
    user_query = parser.parse(query)

    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(
            Sample.id.in_(
                select(IntraHostVariant.sample_id)
                .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
                .join(Translation, Translation.id == IntraHostVariant.translation_id, isouter=True)
                .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    async with get_async_session() as session:
        samples = await session.scalars(samples_query)
        out_data = [SampleInfo.from_db_object(s) for s in samples.unique()]
    return out_data


async def get_samples_accession_and_id_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'select id, accession from samples;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.sample_id})

# todo: all this messing around with divisions to get the index set up right may be unnecessary.
async def get_samples_accession_and_id_as_dask_df():
    # divisions = await _get_sample_accession_divisions()
    uri = get_uri_for_dask()
    query = select(Sample.id, Sample.accession)
    # noinspection PyTypeChecker
    samples: dd.DataFrame = dd.read_sql_query(query, uri, index_col='id')
    samples[StandardColumnNames.sample_id] = samples.index
    return samples

async def _get_sample_accession_divisions(n_divisions: int = 3) -> list[str]:
    """
    Return a list of accessions that will partition samples into N buckets of approximately
    equal size, when sorted by accession.
    todo: this may not actually be necessary
    :param n_divisions: number of buckets desired
    :return: Accessions evenly spaced to produce N or N-1 buckets
    """
    query = f'''
            select {StandardColumnNames.accession}, n from (
            select {StandardColumnNames.accession},
                   (row_number() over (order by {StandardColumnNames.accession} asc)) as n,
                   max_n
            from {TableNames.samples}
                cross join(
                    select count(*) as max_n
                    from {TableNames.samples}
                ) _
            ) acc where
            (acc.n % (select floor(count(*)/{n_divisions})::bigint from {TableNames.samples})) = 0
            or acc.n = 1
            or acc.n = max_n;
            '''
    async with get_async_session() as session:
        res = await session.execute(text(query))

    out = []
    n0 = n1 = n2 = 0
    for acc, n in res:
        n2 = n1
        n1 = n0
        n0 = n
        out.append(acc)
    # Check that the last division is properly spaced
    # This is determined by the second-to-last entry, b/c the last must always be the last sample
    # the last gap must be at least half the size of the preceding one
    if (n0 - n1) / (n1 - n2) < 0.5:
        out = out[:-2] + out[-1:]
    return out

async def get_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_


async def get_sample_collection_release_lag(max_span_days: int) -> List[Dict]:
    start = cast(Sample.collection_start_date, Date)
    end = cast(Sample.collection_end_date, Date)
    release = cast(Sample.release_date, Date)

    half_span = cast((end - start) / 2, Integer)
    midpoint = start + half_span

    year = cast(func.date_part('year', midpoint), Integer)
    month = cast(func.date_part('month', midpoint), Integer)

    lag = release - midpoint

    sub_query = (
        select(
            lag.label("lag"),
            year.label("year"),
            month.label("month")
        )
        .where((end - start) <= max_span_days)
        .subquery()
    )

    query = (
        select(
            sub_query.c.year,
            sub_query.c.month,
            func.percentile_cont(0.25).within_group(sub_query.c.lag).label("q1"),
            func.percentile_cont(0.5).within_group(sub_query.c.lag).label("median"),
            func.percentile_cont(0.75).within_group(sub_query.c.lag).label("q3")
        )
        .group_by(sub_query.c.year, sub_query.c.month)
    )
    date_bin = DateBinOpt("month") # TODO: Generalize this to other intervals
    async with get_async_session() as session:
        result = await session.execute(query)
        if result is None:
            return []
        rows = result.all()
        return [
            {
                "collection_date_bin": date_bin.format_iso_chunk(row.year, row.month),
                "lag_q1": row.q1,
                "lag_median": row.median,
                "lag_q3": row.q3
            }
            for row in rows
        ]

