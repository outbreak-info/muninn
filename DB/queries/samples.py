from typing import List

import polars as pl
from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_uri_for_polars, get_async_session
from DB.models import Sample, Mutation, GeoLocation, Allele, AminoAcid, IntraHostVariant, Translation
from api.models import SampleInfo
from parser.parser import parser
from utils.constants import StandardColumnNames
from utils.errors import NotFoundError


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


async def get_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_
