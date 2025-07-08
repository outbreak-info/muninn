import datetime
from typing import List, Type, Dict

from sqlalchemy import select, text

from DB.engine import get_async_session
from DB.models import PhenotypeMetric, IntraHostVariant, Mutation
from api.models import PhenotypeMetricInfo
from utils.constants import DateBinOpt, NtOrAa
from parser.parser import parser

async def get_all_pheno_metrics() -> List[PhenotypeMetricInfo]:
    async with get_async_session() as session:
        res = await session.scalars(
            select(PhenotypeMetric)
        )
        out_data = [PhenotypeMetricInfo.from_db_object(pm) for pm in res]
    return out_data


async def _count_variants_or_mutations_gte_pheno_value_by_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    phenotype_metric_value_threshold: float,
    days: int,
    max_span_days: int,
    raw_query: str,
    table: Type[IntraHostVariant] | Type[Mutation]
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
                select
                {extract_clause},
                count(distinct sample_id) filter (where value >= {phenotype_metric_value_threshold}) as n_gte,
                count(distinct sample_id) as n
                from(
                    select 
                    value,
                    sample_id,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                        pmv.value as value,
                        s.id as sample_id,
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
                        inner join phenotype_metric_values pmv ON pmv.amino_acid_id = aa.id
                        inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 and pm.name='{phenotype_metric_name}' {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                {group_and_order_clause}
                '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append({
            "date": date,
            "n_gte": r[2],
            "n": r[3]
        })
    return out_data

async def get_phenotype_metric_value_by_variant_quantile(
            phenotype_metric_name: str,
            quantile: float) -> Dict[str, float]:
    return await _get_phenotype_metric_value_quantile(phenotype_metric_name, quantile, IntraHostVariant)

async def get_phenotype_metric_value_by_mutation_quantile(
            phenotype_metric_name: str,
            quantile: float) -> Dict[str, float]:
    return await _get_phenotype_metric_value_quantile(phenotype_metric_name, quantile, Mutation)

async def _get_phenotype_metric_value_quantile(
        phenotype_metric_name: str,
        quantile: float,
        by_table: Type[IntraHostVariant] | Type[Mutation]) -> Dict[str, float]:
    query = f"""
    SELECT percentile_disc({quantile}) within group (order by pmv.value)
        FROM amino_acids aa
        INNER JOIN translations t ON t.amino_acid_id = aa.id
        INNER JOIN {by_table.__tablename__} vm ON vm.translation_id = t.id
        INNER JOIN samples s on s.id = vm.sample_id
        INNER JOIN samples_lineages sl on sl.sample_id = s.id
        INNER JOIN lineages l ON l.id = sl.lineage_id
        INNER JOIN lineage_systems ls on ls.id = l.lineage_system_id
        INNER JOIN phenotype_metric_values pmv ON pmv.amino_acid_id = aa.id
        INNER JOIN phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
    WHERE pm.name='{phenotype_metric_name}' AND pmv.value != 0;
    """
    async with get_async_session() as session:
        res = await session.scalars(
            text(query)
        )
    value = next(res)
    return {
        "quantile": quantile,
        "phenotype_metric_value": value
    }