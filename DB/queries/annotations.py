import datetime
from collections import defaultdict
from typing import Type, List, Dict

from sqlalchemy import text

from DB.engine import get_async_session
from DB.models import IntraHostVariant, Mutation
from parser.parser import parser
from utils.constants import DateBinOpt

# TODO: Add Effect to DB.models and EffectInfo to api.models
async def get_all_annotation_effects() -> List[str]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                '''
                select e.detail
                from effects e;
                '''
            )
        )
    out_data = []
    for r in res:
        out_data.append(r[0])
    return out_data


async def get_annotations_by_mutations_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    raw_query: str,
) -> List[Dict]:
    return await _get_annotations_by_collection_date(effect_detail, date_bin, days, max_span_days, raw_query, Mutation)


async def get_annotations_by_variants_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    raw_query: str,
) -> List[Dict]:
    return await _get_annotations_by_collection_date(
        effect_detail,
        date_bin,
        days,
        max_span_days,
        raw_query,
        IntraHostVariant
    )


async def _get_annotations_by_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    raw_query: str,
    table: Type[IntraHostVariant] | Type[Mutation]
) -> List[Dict]:
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
            group by year, chunk
            order by year, chunk
            '''
        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            group_and_order_clause = f'''
            group by bin_start, bin_end
            order by bin_start
            '''
        case _:
            raise ValueError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                WITH base as (
                    select 
                    aa_id,
                    detail,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                        aa.id as aa_id,
                        e.detail,
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join {table.__tablename__} VM on VM.sample_id = s.id
                        inner join samples_lineages sl on sl.sample_id = s.id
                        inner join lineages l ON l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        left join translations t on t.id = VM.translation_id
                        left join amino_acids aa on aa.id = t.amino_acid_id
                        inner join annotations_amino_acids aaa on aaa.amino_acid_id = aa.id
                        inner join annotations a on a.id = aaa.annotation_id
                        inner join effects e on e.id = a.effect_id
                        inner join annotations_papers ap on ap.annotation_id = a.id
                        where num_nulls(collection_end_date, collection_start_date) = 0 {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                select
                {extract_clause},
                count(DISTINCT aa_id) FILTER (where detail = :effect_detail) as n,
                COUNT(DISTINCT aa_id) as n_total,
                COUNT(DISTINCT aa_id) FILTER (WHERE detail = :effect_detail)::numeric
                    / NULLIF(COUNT(DISTINCT aa_id), 0) AS proportion
                from BASE
                {group_and_order_clause}
                '''
            ),
            {
                'effect_detail': effect_detail
            }
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append({
            "date": date,
            "n": r[2],
            "n_total": r[3],
            "proportion": r[4]
        })
    return out_data

async def get_annotations_by_variants_and_amino_acid_position(
    effect_detail: str,
    raw_query: str
) -> Dict:
    return await _get_annotations_by_amino_acid_position(
        effect_detail,
        raw_query,
        IntraHostVariant
    )

async def get_annotations_by_mutations_and_amino_acid_position(
    effect_detail: str,
    raw_query: str
) -> Dict:
    return await _get_annotations_by_amino_acid_position(
        effect_detail,
        raw_query,
        Mutation
    )

async def _get_annotations_by_amino_acid_position(
    effect_detail: str,
    raw_query: str,
    table: Type[IntraHostVariant] | Type[Mutation]
) -> Dict:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                SELECT aa.gff_feature, aa.position_aa, aa.alt_aa, aa.ref_aa, COUNT(*)  FROM amino_acids aa
                    inner join translations t on t.amino_acid_id = aa.id
                    inner join annotations_amino_acids aaa on aaa.amino_acid_id = aa.id
                    inner join annotations a on a.id = aaa.annotation_id
                    inner join {table.__tablename__} VM on VM.translation_id = t.id
                    inner join samples s on VM.sample_id = s.id
                    inner join samples_lineages sl on sl.sample_id = s.id
                    inner join lineages l ON l.id = sl.lineage_id
                    inner join lineage_systems ls on ls.id = l.lineage_system_id
                    inner join effects e on e.id = a.effect_id
                    inner join annotations_papers ap on ap.annotation_id = a.id
                    where e.detail = :effect_detail {user_where_clause}
                GROUP BY aa.position_aa, aa.gff_feature, aa.alt_aa, aa.ref_aa;
                '''
            ),
            {
                'effect_detail': effect_detail
            }
        )
    out = defaultdict(list)
    for r in res:
        gff_feature, position_aa, alt_aa, ref_aa, count = r
        out[gff_feature].append({
            "position_aa": position_aa,
            "alt_aa": alt_aa,
            "ref_aa": ref_aa,
            "count": count
        })
    return out
