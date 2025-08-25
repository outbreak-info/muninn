import datetime
from typing import List

import polars as pl
from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

import DB.queries.variants_mutations
from DB.engine import get_async_session, get_uri_for_polars
from DB.models import Mutation, Allele, AminoAcid, Sample, GeoLocation, Translation
from api.models import MutationInfo, RegionAndGffFeatureInfo
from parser.parser import parser
from utils.constants import StandardColumnNames, DateBinOpt


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
        .join(Translation, Translation.id == Mutation.translation_id, isouter=True)
        .options(contains_eager(Mutation.r_translation))
        .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
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

async def get_region_and_gff_features() -> List['RegionAndGffFeatureInfo']:
    return await DB.queries.variants_mutations.get_region_and_gff_features(Mutation)

# TODO: Generalize this for nucleotide mutations
async def get_aa_mutation_count_by_simple_date_bin(
    date_bin: DateBinOpt,
    position_aa: int,
    alt_aa: str,
    gff_feature: str,
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
                gff_feature,
                ref_aa,
                position_aa,
                alt_aa,
                lineage_name
                from(
                    select 
                    gff_feature,
                    ref_aa,
                    position_aa,
                    alt_aa,
                    sample_id,
                    lineage_name,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                            aa.gff_feature, 
                            aa.ref_aa, 
                            aa.position_aa, 
                            aa.alt_aa,  
                            s.id as sample_id, 
                            s.collection_start_date, 
                            s.collection_end_date,
                            l.lineage_name,
                            collection_end_date - collection_start_date as collection_span
                        from samples s
                        inner join mutations m on m.sample_id = s.id
                        inner join translations t on t.id = m.translation_id
                        inner join amino_acids aa on aa.id = t.amino_acid_id
                        inner join samples_lineages sl on sl.sample_id = s.id
                        inner join lineages l on l.id = sl.lineage_id
                        where aa.position_aa = {position_aa} and aa.alt_aa='{alt_aa}' and gff_feature='{gff_feature}' {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                group by gff_feature, ref_aa, position_aa, alt_aa, lineage_name, {group_and_order_clause}
                '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append({
            "date": date,
            "n": r[2],
            "gff_feature": r[3],
            "ref_aa": r[4],
            "position_aa": r[5],
            "alt_aa": r[6],
            "lineage_name": r[7]
        })
    return out_data