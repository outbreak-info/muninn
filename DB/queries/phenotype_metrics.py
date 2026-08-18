from typing import List, Type, Dict

from sqlalchemy import select, text, func

from DB.engine import get_async_session
from DB.models import PhenotypeMetric, IntraHostVariant, Mutation, PhenotypeMetricValues
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION, YEAR, CHUNK, BIN_START, BIN_END
from DB.queries.helpers import get_appropriate_translations_table_and_id
from api.models import PhenotypeMetricInfo
from parser.parser import parser
from utils.constants import DateBinOpt, COLLECTION_DATE, ColumnNames, TableNames


async def get_all_pheno_metrics() -> List[PhenotypeMetricInfo]:
    query = f'''
        select
            id,
            {ColumnNames.phenotype_metric_name} as name,
            {ColumnNames.phenotype_metric_assay_type} as assay_type
        from {TableNames.phenotype_metrics}
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        out_data = [PhenotypeMetricInfo(**row) for row in res.mappings().all()]
    return out_data


async def get_min_max_pheno_metric_value(phenotype_metric_name: str) -> List:
    query = f"""
        select min(pmv.value) as min_value, max(pmv.value) as max_value
        from {TableNames.phenotype_metric_values} pmv
        inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
        where pm.{ColumnNames.phenotype_metric_name} = :pm_name
    """
    async with get_async_session() as session:
        res = await session.execute(text(query), {'pm_name': phenotype_metric_name})
        row = res.one_or_none()
    if row is None:
        return [None, None]
    min_val, max_val = row
    return [min_val, max_val]


async def count_variants_or_mutations_gte_pheno_value_by_collection_date(
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

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

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
                    {MID_COLLECTION_DATE_CALCULATION}
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
                        where num_nulls(collection_end_date, collection_start_date) = 0
                        and pm.{ColumnNames.phenotype_metric_name} = :pm_name
                        {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            ),
            {
                'pm_name': phenotype_metric_name
            }
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


async def count_mutations_gte_pheno_value_by_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    phenotype_metric_value_threshold: float,
    days: int,
    max_span_days: int,
    filter: str | None,
) -> List[Dict]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            bin_select_cols = f'{YEAR}, {CHUNK}'
        case DateBinOpt.day:
            bin_select_cols = f'{BIN_END}, {BIN_START}'
        case _:
            raise NotImplementedError

    query = f'''
        with binned_samples as (
            select s.id as sample_id,
                {MID_COLLECTION_DATE_CALCULATION}
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            inner join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            where num_nulls({ColumnNames.collection_end_date}, {ColumnNames.collection_start_date}) = 0
              and ({ColumnNames.collection_end_date} - {ColumnNames.collection_start_date}) <= :max_span_days
              {user_defined_filter}
        ),
        bins as (
            select {extract_clause},
                   rb_build_agg(sample_id) as bm
            from binned_samples
            {group_by_clause}
        ),
        scored as (
            select pmv.{ColumnNames.amino_acid_id} as aa_id, pmv.value as value
            from {TableNames.phenotype_metric_values} pmv
            inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
            where pm.{ColumnNames.phenotype_metric_name} = :pm_name
        ),
        per as (
            select {bin_select_cols},
                   sc.value as value,
                   rb_and_cardinality(m.{ColumnNames.samples_present}, b.bm) as card
            from bins b
            cross join scored sc
            inner join {TableNames.cns_samples_by_amino_acid} m on m.{ColumnNames.amino_acid_id} = sc.aa_id
        )
        select {bin_select_cols},
               count(*) filter (where card > 0 and value >= :threshold) as n_gte,
               count(*) filter (where card > 0) as n
        from per
        {group_by_clause}
        having count(*) filter (where card > 0) > 0
        {order_by_clause}
    '''
    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'pm_name': phenotype_metric_name,
                'threshold': phenotype_metric_value_threshold,
                'max_span_days': max_span_days,
            }
        )
        rows = res.all()
    out_data = []
    for r in rows:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "n_gte": r[2],
                "n": r[3]
            }
        )
    return out_data


async def count_variants_gte_pheno_value_by_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    phenotype_metric_value_threshold: float,
    days: int,
    max_span_days: int,
    filter: str | None,
    min_alt_freq: float | None = None,
    max_alt_freq: float | None = None,
) -> List[Dict]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            bin_select_cols = f'{YEAR}, {CHUNK}'
        case DateBinOpt.day:
            bin_select_cols = f'{BIN_END}, {BIN_START}'
        case _:
            raise NotImplementedError

    query = f'''
        with binned_samples as (
            select s.id as sample_id,
                {MID_COLLECTION_DATE_CALCULATION}
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            inner join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            where num_nulls({ColumnNames.collection_end_date}, {ColumnNames.collection_start_date}) = 0
              and ({ColumnNames.collection_end_date} - {ColumnNames.collection_start_date}) <= :max_span_days
              {user_defined_filter}
        ),
        bins as (
            select {extract_clause},
                   rb_build_agg(sample_id) as bm
            from binned_samples
            {group_by_clause}
        ),
        scored as (
            select pmv.{ColumnNames.amino_acid_id} as aa_id, pmv.value as value
            from {TableNames.phenotype_metric_values} pmv
            inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
            where pm.{ColumnNames.phenotype_metric_name} = :pm_name
        ),
        carriers as (
            select v.{ColumnNames.amino_acid_id} as aa_id,
                   rb_or_agg(v.{ColumnNames.samples_present}) as bm
            from {TableNames.ih_samples_by_amino_acid} v
            inner join scored sc on sc.aa_id = v.{ColumnNames.amino_acid_id}
            where v.alt_freq_range && numrange(:min_alt_freq, :max_alt_freq, '[]')
            group by v.{ColumnNames.amino_acid_id}
        ),
        per as (
            select {bin_select_cols},
                   sc.value as value,
                   rb_and_cardinality(ca.bm, b.bm) as card
            from bins b
            cross join scored sc
            inner join carriers ca on ca.aa_id = sc.aa_id
        )
        select {bin_select_cols},
               count(*) filter (where card > 0 and value >= :threshold) as n_gte,
               count(*) filter (where card > 0) as n
        from per
        {group_by_clause}
        having count(*) filter (where card > 0) > 0
        {order_by_clause}
    '''
    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'pm_name': phenotype_metric_name,
                'threshold': phenotype_metric_value_threshold,
                'max_span_days': max_span_days,
                'min_alt_freq': min_alt_freq,
                'max_alt_freq': max_alt_freq,
            }
        )
        rows = res.all()
    out_data = []
    for r in rows:
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
) -> Dict[str, float | None]:
    query = f"""
        select percentile_disc(:quantile) within group (order by pmv.value)
        from {TableNames.phenotype_metric_values} pmv
        inner join {TableNames.phenotype_metrics} pm on pm.id = pmv.{ColumnNames.phenotype_metric_id}
        where pm.{ColumnNames.phenotype_metric_name} = :pm_name and pmv.value != 0
    """
    async with get_async_session() as session:
        res = await session.scalars(
            text(query),
            {'pm_name': phenotype_metric_name, 'quantile': quantile}
        )
        value = res.first()
    return {
        "quantile": quantile,
        "phenotype_metric_value": value
    }


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
            WHERE pm.{ColumnNames.phenotype_metric_name} = :pm_name
            AND pmv.value != 0;
            """
    async with get_async_session() as session:
        res = await session.scalars(
            text(query),
            {'pm_name': phenotype_metric_name}
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
    raise NotImplementedError(
        "This function is not implemented yet."
    )
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
    filter: str | None,
) -> List[Dict]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    query = (
        f'with matching_samples as (\n'
        f'    select s.id as sample_id,\n'
        f'           {MID_COLLECTION_DATE_CALCULATION}\n'
        f'    from samples s\n'
        f'    left join geo_locations gl on gl.id = s.geo_location_id\n'
        f'    inner join samples_lineages sl on sl.sample_id = s.id\n'
        f'    inner join lineages l on l.id = sl.lineage_id\n'
        f'    inner join lineage_systems ls on ls.id = l.lineage_system_id\n'
        f'    where (s.collection_end_date - s.collection_start_date) <= :max_span_days {user_defined_filter}\n'
        f')\n'
        f'select {extract_clause},\n'
        f'       percentile_cont(0.25) within group (order by pmv_value_sum) as pmv_sum_q1,\n'
        f'       percentile_cont(0.5) within group (order by pmv_value_sum) as pmv_sum_median,\n'
        f'       percentile_cont(0.75) within group (order by pmv_value_sum) as pmv_sum_q3,\n'
        f'       percentile_cont(0.25) within group (order by n_scored_mutations) as n_scores_q1,\n'
        f'       percentile_cont(0.5) within group (order by n_scored_mutations) as n_scores_median,\n'
        f'       percentile_cont(0.75) within group (order by n_scored_mutations) as n_scores_q3\n'
        f'from matching_samples\n'
        f'inner join cache_cns_pmv_sums cache using (sample_id)\n'
        f'inner join phenotype_metrics pm on pm.id = cache.phenotype_metric_id\n'
        f'where pm.phenotype_metric_name = :pm_name\n'
        f'{group_by_clause}\n'
        f'{order_by_clause};'
    )
    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'pm_name': phenotype_metric_name,
                'max_span_days': max_span_days,
            }
        )
        rows = res.all()
    out_data = []
    for r in rows:
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

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    # Todo fix for intrahost variants, this is currently only for mutations
    translations_table, translations_join_col = (
        get_appropriate_translations_table_and_id(table)
    )

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
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
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                        pmv.value as value,
                        s.id as sample_id,
                        aa.id as aa_id,
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from mutation_translations mt
                        inner join amino_acids aa on aa.id = mt.amino_acid_id
                        inner join phenotype_metric_values pmv on pmv.amino_acid_id = aa.id
                        inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                        cross join lateral unnest(rb_to_array(mt.sequences_present)) as seqs(sequence_id)
                        inner join samples s on s.sequence_id = seqs.sequence_id
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join samples_lineages sl on sl.sample_id = s.id
                        inner join lineages l on l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        where num_nulls(collection_end_date, collection_start_date) = 0
                        and pm.{ColumnNames.phenotype_metric_name}=:pm_name
                        {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                    group by sample_id, collection_start_date, collection_end_date
                )
                {group_by_clause}
                {order_by_clause}
                """
            ),
            {"pm_name": phenotype_metric_name},
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
