from typing import List

import polars as pl
from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session, get_uri_for_polars
from DB.models import Mutation, Allele, AminoAcid, Sample, GeoLocation, Translation
from api.models import MutationInfo
from parser.parser import parser
from utils.constants import StandardColumnNames


async def get_mutations(query: str) -> List['MutationInfo']:
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, Translation, AminoAcid)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(Translation, Translation.id == Mutation.translation_id, isouter=True)
        .options(contains_eager(Mutation.r_translation))
        .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
        .options(contains_eager(Translation.r_amino_acid))
        .where(
            text(user_query)
        )
    )

    async with get_async_session() as session:
        results = await session.scalars(mutations_query)
        out_data = [MutationInfo.from_db_object(m) for m in results.unique()]
    return out_data


async def get_mutations_by_sample(query: str) -> List['MutationInfo']:
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, Translation, AminoAcid)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .options(contains_eager(Allele.r_translations))
        .join(AminoAcid, Translation.amino_acid_substitution_id == AminoAcid.id, isouter=True)
        .options(contains_eager(Translation.r_amino_acid))
        .where(
            Mutation.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    async with get_async_session() as session:
        results = await session.scalars(mutations_query)
        out_data = [MutationInfo.from_db_object(m) for m in results.unique()]
    return out_data


async def get_all_mutations_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query='select * from mutations;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.mutation_id})
