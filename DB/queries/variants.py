import datetime
from typing import List

import polars as pl
from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session, get_uri_for_polars
from DB.models import Sample, IntraHostVariant, Allele, AminoAcid, GeoLocation, Translation
from api.models import VariantInfo, RegionAndGffFeatureInfo
from parser.parser import parser
from utils.constants import StandardColumnNames, DateBinOpt
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

# TODO: Generalize this for nucleotide mutations
async def get_aa_variant_frequency_by_simple_date_bin(
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    raw_query: str
):
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from mid_collection_date) as year,
            extract({date_bin} from mid_collection_date) as chunk
            '''

            group_and_order_clause = f'''
            year, chunk
            order by year, chunk
            '''
        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            group_and_order_clause = f'''
            bin_start, bin_end
            order by bin_start
            '''
        case _:
            raise ValueError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(distinct sample_id) as n,
                percentile_cont(0.25) within group (order by alt_freq) as q1,
                percentile_cont(0.5) within group (order by alt_freq) as median,
                percentile_cont(0.75) within group (order by alt_freq) as q3,
                gff_feature,
                ref_aa,
                position_aa,
                alt_aa
                from(
                    select 
                    gff_feature,
                    ref_aa,
                    position_aa,
                    alt_aa,
                    alt_freq,
                    sample_id,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                            aa.gff_feature, 
                            aa.ref_aa, 
                            aa.position_aa, 
                            aa.alt_aa, 
                            ihv.alt_freq, 
                            s.id as sample_id, 
                            s.collection_start_date, 
                            s.collection_end_date,
                            collection_end_date - collection_start_date as collection_span
                        from samples s
                        inner join intra_host_variants ihv on ihv.sample_id = s.id
                        inner join translations t on t.id = ihv.translation_id
                        inner join amino_acids aa on aa.id = t.amino_acid_id
                        left join samples_lineages sl on sl.sample_id = s.id
                        left join lineages l on l.id = sl.lineage_id
                        left join lineage_systems ls on ls.id = l.lineage_system_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                group by gff_feature, ref_aa, position_aa, alt_aa, {group_and_order_clause}
                '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append({
            "date": date,
            "n": r[2],
            "alt_freq_q1": r[3],
            "alt_freq_median": r[4],
            "alt_freq_q3": r[5],
            "gff_feature": r[6],
            "ref_aa": r[7],
            "position_aa": r[8],
            "alt_aa": r[9]
        })
    return out_data