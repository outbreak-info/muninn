from typing import List

import polars as pl
from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session, get_uri_for_polars
from DB.models import Sample, IntraHostVariant, Allele, AminoAcid, GeoLocation, Translation
from api.models import VariantInfo, RegionAndGffFeatureInfo
from parser.parser import parser
from utils.constants import StandardColumnNames
import DB.queries.variants_mutations

async def get_variants(query: str) -> List['VariantInfo']:
    user_query = parser.parse(query)

    variants_query = (
        select(IntraHostVariant, Allele, Translation, AminoAcid)
        .join(Allele, IntraHostVariant.allele_id == Allele.id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_allele))
        .join(Translation, Translation.id == IntraHostVariant.translation_id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_translation))
        .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
        .options(contains_eager(Translation.r_amino_acid))
        .where(text(user_query))
    )

    async with get_async_session() as session:
        variants = await session.scalars(variants_query)
        out_data = [VariantInfo.from_db_object(v) for v in variants.unique()]
    return out_data


async def get_variants_for_sample(query: str) -> List['VariantInfo']:
    user_query = parser.parse(query)
    variants_query = (
        select(IntraHostVariant, Allele, Translation, AminoAcid)
        .join(Allele, IntraHostVariant.allele_id == Allele.id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_allele))
        .join(Translation, Translation.id == IntraHostVariant.translation_id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_translation))
        .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
        .options(contains_eager(Translation.r_amino_acid))
        .filter(
            IntraHostVariant.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    async with get_async_session() as session:
        results = await session.scalars(variants_query)
        out_data = [VariantInfo.from_db_object(v) for v in results.unique()]
    return out_data


async def get_all_variants_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'select * from {IntraHostVariant.__tablename__};',
        uri=get_uri_for_polars()
    ).rename(
        {'id': StandardColumnNames.intra_host_variant_id}
    )

async def get_region_and_gff_features() -> List['RegionAndGffFeatureInfo']:
        return await DB.queries.variants_mutations.get_region_and_gff_features(IntraHostVariant)
