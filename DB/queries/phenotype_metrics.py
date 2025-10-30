import datetime
from typing import List, Type, Dict

from sqlalchemy import select, text, func

from DB.engine import get_async_session
from DB.models import PhenotypeMetric, IntraHostVariant, Mutation, PhenotypeMetricValues
from DB.queries.helpers import get_appropriate_translations_table_and_id
from api.models import PhenotypeMetricInfo
from parser.parser import parser
from utils.constants import DateBinOpt


async def get_all_pheno_metrics() -> List[PhenotypeMetricInfo]:
    async with get_async_session() as session:
        res = await session.scalars(
            select(PhenotypeMetric)
        )
        out_data = [PhenotypeMetricInfo.from_db_object(pm) for pm in res]
    return out_data


async def get_min_max_pheno_metric_value(phenotype_metric_name: str) -> List:
    async with get_async_session() as session:
        res = await session.execute(
            select(
                func.min(PhenotypeMetricValues.value),
                func.max(PhenotypeMetricValues.value)
            )
            .join(PhenotypeMetric, PhenotypeMetricValues.phenotype_metric_id == PhenotypeMetric.id)
            .where(PhenotypeMetric.name == phenotype_metric_name)
        )
        row = res.one_or_none()
        if row is None:
            return [None, None]
        min_val, max_val = row
        return [min_val, max_val]


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

    translations_table, translations_join_id_col = get_appropriate_translations_table_and_id(table)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(distinct aa_id) filter (where value >= {phenotype_metric_value_threshold}) as n_gte,
                count(distinct aa_id) as n
                from(
                    select 
                    value,
                    aa_id,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                        pmv.value as value,
                        aa.id as aa_id,
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join {table.__tablename__} VM on VM.sample_id = s.id
                        inner join samples_lineages sl on sl.sample_id = s.id
                        inner join lineages l ON l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        left join {translations_table} t on t.{translations_join_id_col} = VM.id
                        left join amino_acids aa on aa.id = t.amino_acid_id
                        inner join phenotype_metric_values pmv ON pmv.amino_acid_id = aa.id
                        inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 and pm.name='{phenotype_metric_name}' 
                        {user_where_clause}
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
        out_data.append(
            {
                "date": date,
                "n_gte": r[2],
                "n": r[3]
            }
        )
    return out_data


async def get_phenotype_metric_value_by_variant_quantile(
    phenotype_metric_name: str,
    quantile: float
) -> Dict[str, float]:
    return await _get_phenotype_metric_value_quantile(phenotype_metric_name, quantile, IntraHostVariant)


async def get_phenotype_metric_value_by_mutation_quantile(
    phenotype_metric_name: str,
    quantile: float
) -> Dict[str, float]:
    return await _get_phenotype_metric_value_quantile(phenotype_metric_name, quantile, Mutation)


async def _get_phenotype_metric_value_quantile(
    phenotype_metric_name: str,
    quantile: float,
    by_table: Type[IntraHostVariant] | Type[Mutation]
) -> Dict[str, float]:
    translations_table, translations_join_id_col = get_appropriate_translations_table_and_id(by_table)
    query = f"""
            SELECT percentile_disc({quantile}) within group (order by pmv.value)
                FROM amino_acids aa
                INNER JOIN {translations_table} t ON t.amino_acid_id = aa.id
                INNER JOIN {by_table.__tablename__} vm ON vm.id = t.{translations_join_id_col}
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


async def get_pheno_value_for_variants_by_sample_and_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        raw_query,
        IntraHostVariant
    )


async def get_pheno_value_for_mutations_by_sample_and_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        raw_query,
        Mutation
    )


async def _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
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

    translations_table, translations_join_col = get_appropriate_translations_table_and_id(table)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                percentile_cont(0.25) within group (order by aggregate_value) as q1,
                percentile_cont(0.5) within group (order by aggregate_value) as median,
                percentile_cont(0.75) within group (order by aggregate_value) as q3,
                percentile_cont(0.25) within group (order by n_amino_acid_mutations) as q1_aa,
                percentile_cont(0.5) within group (order by n_amino_acid_mutations) as median_aa,
                percentile_cont(0.75) within group (order by n_amino_acid_mutations) as q3_aa
                from(
                    select 
                    SUM(value) as aggregate_value,
                    count(distinct aa_id) as n_amino_acid_mutations,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                        pmv.value as value,
                        s.id as sample_id,
                        aa.id as aa_id,
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join {table.__tablename__} VM on VM.sample_id = s.id
                        inner join samples_lineages sl on sl.sample_id = s.id
                        inner join lineages l ON l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        left join {translations_table} t on t.{translations_join_col} = VM.id
                        left join amino_acids aa on aa.id = t.amino_acid_id
                        inner join phenotype_metric_values pmv ON pmv.amino_acid_id = aa.id
                        inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 and pm.name='{phenotype_metric_name}' {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                    group by sample_id, collection_start_date, collection_end_date
                )
                {group_and_order_clause}
                '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "aggregate_value_q1": r[2],
                "aggregate_value_median": r[3],
                "aggregate_value_q3": r[4],
                "n_aa_q1": r[5],
                "n_aa_median": r[6],
                "n_aa_q3": r[7],
            }
        )
    return out_data
