from typing import List, Dict

from sqlalchemy import select, text, cast, Date, func, Integer
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Sample, Mutation, GeoLocation, Allele, AminoAcid, IntraHostVariant, MutationTranslation, \
    IntraHostTranslation
from api.models import SampleInfo
from parser.parser import parser
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
                .join(MutationTranslation, MutationTranslation.mutation_id== Mutation.id, isouter=True)
                .join(AminoAcid, AminoAcid.id == MutationTranslation.amino_acid_id, isouter=True)
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
                .join(IntraHostTranslation, IntraHostTranslation.intra_host_variant_id == IntraHostVariant.id, isouter=True)
                .join(AminoAcid, AminoAcid.id == IntraHostTranslation.amino_acid_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    async with get_async_session() as session:
        samples = await session.scalars(samples_query)
        out_data = [SampleInfo.from_db_object(s) for s in samples.unique()]
    return out_data


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

