import re
from typing import List, Type, Dict

from sqlalchemy import select, text, func

from DB.engine import get_async_session
from DB.models import PhenotypeMetric, IntraHostVariant, Mutation, PhenotypeMetricValues
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from DB.queries.helpers import get_appropriate_translations_table_and_id
from api.models import PhenotypeMetricInfo
from parser.parser import parser
from utils.constants import DateBinOpt, COLLECTION_DATE, StandardColumnNames


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
            .where(PhenotypeMetric.phenotype_metric_name == phenotype_metric_name)
        )
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
                        and pm.{StandardColumnNames.phenotype_metric_name} = :pm_name 
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
            WHERE pm.{StandardColumnNames.phenotype_metric_name} = :pm_name 
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
    lineage_system_name: str,
    background: str,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        lineage_system_name,
        background,
        days,
        max_span_days,
        raw_query,
        IntraHostVariant
    )


async def get_pheno_value_for_mutations_by_sample_and_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    lineage_system_name: str,
    background: str,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        lineage_system_name,
        background,
        days,
        max_span_days,
        raw_query,
        Mutation
    )


async def _pheno_value_for_mutations_or_variants_by_sample_and_collection_date(
    date_bin: DateBinOpt,
    phenotype_metric_name: str,
    lineage_system_name: str,
    background: str,
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

    translations_table, translations_join_col = get_appropriate_translations_table_and_id(table)

    async with get_async_session() as session:
        refs = await session.execute(
            text(
                f'''
                SELECT DISTINCT a.region 
                FROM alleles a
                '''
            )
        )
        refs_ls = [row[0] for row in refs.fetchall()]
        refs_ls = [m.group(1) if (m := re.match(r'^NC_045512\.2_(.+)_(?:rbd|spike)$', r)) else r for r in refs_ls]

        if background is not None and background in refs_ls:
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
                    percentile_cont(0.75) within group (order by n_amino_acid_mutations) as q3_aa,
                    :background AS ref
                    from(
                        WITH allele_subset AS (
                            SELECT * FROM alleles
                            WHERE region = :background
                        )
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
                            from samples s
                            inner join {table.__tablename__} VM on VM.sample_id = s.id
                            INNER JOIN allele_subset als ON als.id = VM.allele_id
                            inner join samples_lineages sl on sl.sample_id = s.id
                            inner join lineages l ON l.id = sl.lineage_id
                            inner join lineage_systems ls on ls.id = l.lineage_system_id
                            left join {translations_table} t on t.{translations_join_col} = VM.id
                            left join amino_acids aa on aa.id = t.amino_acid_id
                            inner join phenotype_metric_values pmv ON pmv.amino_acid_id = aa.id
                            inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                            where num_nulls(collection_end_date, collection_start_date) = 0 
                            and pm.{StandardColumnNames.phenotype_metric_name}=:pm_name
                            AND ls.lineage_system_name = :lineage_system_name
                            {user_where_clause}
                        )
                        where collection_span <= {max_span_days}
                        group by sample_id, collection_start_date, collection_end_date
                    )
                    {group_by_clause}
                    {order_by_clause}
                    '''
                ),
                {
                    'pm_name': phenotype_metric_name,
                    'background': background,
                    'lineage_system_name': lineage_system_name,
                }
            )
        else:
            # use _rbd in the expression since we only have dms value in RBD region
            # evescape is for spike region. this still works (?) since it is Hu-1 only, i.e. all samples will use Hu-1 as background ref.
            # COALESCE by NC_045512.2 since Hu-1 is not in the .yaml file
            res = await session.execute(
                text(
                    f'''
                    WITH RECURSIVE
                    ref_lineages AS (
                        SELECT rc.ref_name, l.id AS lineage_id
                        FROM (VALUES {", ".join(f"('{ref}')" for ref in refs_ls)}) AS rc(ref_name)
                        INNER JOIN lineages l ON l.lineage_name = rc.ref_name
                        INNER JOIN lineage_systems ls ON l.lineage_system_id = ls.id
                        WHERE ls.lineage_system_name = 'freyja_demixed' -- lineages_immediate_children is linked to freyja system only, hard-coded for now
                    ),
                    distances_down AS (
                        SELECT lineage_id AS current_lineage_id, ref_name, 0 AS depth
                        FROM ref_lineages

                        UNION ALL

                        SELECT lic.child_id, d.ref_name, d.depth + 1
                        FROM distances_down d
                        JOIN lineages_immediate_children lic ON lic.parent_id = d.current_lineage_id
                    ),
                    distances_up AS (
                        SELECT lineage_id AS current_lineage_id, ref_name, 0 AS depth
                        FROM ref_lineages

                        UNION ALL

                        SELECT lic.parent_id, d.ref_name, d.depth + 1
                        FROM distances_up d
                        JOIN lineages_immediate_children lic ON lic.child_id = d.current_lineage_id
                    ),
                    closest_match AS MATERIALIZED (
                        SELECT
                            l.id AS lineage_id,
                            COALESCE(y.closest_ref, 'NC_045512.2') AS closest_ref
                        FROM lineages l
                        LEFT JOIN (
                            SELECT DISTINCT ON (current_lineage_id)
                                current_lineage_id AS lineage_id,
                                ref_name AS closest_ref
                            FROM (
                                SELECT * FROM distances_down
                                UNION ALL
                                SELECT * FROM distances_up
                            ) all_distances
                            ORDER BY current_lineage_id, depth ASC
                        ) y ON l.id = y.lineage_id
                    ),
                    alleles_filtered AS MATERIALIZED (
                        SELECT a.id,
                            COALESCE(
                                substring(a.region FROM :region_pattern),
                                a.region
                            ) AS normalized_region
                        FROM alleles a
                        WHERE COALESCE(
                                substring(a.region FROM :region_pattern),
                                a.region
                            ) IN (SELECT DISTINCT closest_ref FROM closest_match)
                    ),
                    pmv_filtered AS MATERIALIZED (
                        SELECT pmv.amino_acid_id, pmv.value
                        FROM phenotype_metric_values pmv
                        INNER JOIN phenotype_metrics pm ON pm.id = pmv.phenotype_metric_id
                        WHERE pm.{StandardColumnNames.phenotype_metric_name} = :pm_name
                    ),
                    inner_query AS (
                        SELECT
                            translated.value AS value,
                            s.id AS sample_id,
                            translated.aa_id AS aa_id,
                            collection_start_date, collection_end_date,
                            collection_end_date - collection_start_date AS collection_span
                        FROM samples s
                        INNER JOIN samples_lineages sl ON sl.sample_id = s.id
                        INNER JOIN closest_match cm ON cm.lineage_id = sl.lineage_id
                        INNER JOIN {table.__tablename__} VM ON VM.sample_id = s.id
                        INNER JOIN alleles_filtered af ON af.id = VM.allele_id
                            AND af.normalized_region = cm.closest_ref
                        INNER JOIN (
                            SELECT t.mutation_id, pmv.value, aa.id AS aa_id
                            FROM {translations_table} t
                            INNER JOIN amino_acids aa ON aa.id = t.amino_acid_id
                            INNER JOIN pmv_filtered pmv ON pmv.amino_acid_id = aa.id
                        ) translated ON translated.{translations_join_col} = VM.id
                        INNER JOIN lineages l ON l.id = sl.lineage_id
                        INNER JOIN lineage_systems ls ON ls.id = l.lineage_system_id
                        WHERE num_nulls(collection_end_date, collection_start_date) = 0
                            AND ls.lineage_system_name = :lineage_system_name
                        {user_where_clause}
                    ),
                    per_sample AS (
                        SELECT
                            SUM(value) AS aggregate_value,
                            count(distinct aa_id) AS n_amino_acid_mutations,
                            {MID_COLLECTION_DATE_CALCULATION}
                        FROM inner_query
                        WHERE collection_span <= {max_span_days}
                        GROUP BY sample_id, collection_start_date, collection_end_date
                    )
                    SELECT
                        {extract_clause},
                        percentile_cont(0.25) WITHIN GROUP (ORDER BY aggregate_value) AS q1,
                        percentile_cont(0.5)  WITHIN GROUP (ORDER BY aggregate_value) AS median,
                        percentile_cont(0.75) WITHIN GROUP (ORDER BY aggregate_value) AS q3,
                        percentile_cont(0.25) WITHIN GROUP (ORDER BY n_amino_acid_mutations) AS q1_aa,
                        percentile_cont(0.5)  WITHIN GROUP (ORDER BY n_amino_acid_mutations) AS median_aa,
                        percentile_cont(0.75) WITHIN GROUP (ORDER BY n_amino_acid_mutations) AS q3_aa,
                        :background AS ref
                    FROM per_sample
                    {group_by_clause}
                    {order_by_clause}
                    '''
                ),
                {
                    'pm_name': phenotype_metric_name,
                    'background': "closest reference",
                    'lineage_system_name': lineage_system_name,
                    'region_pattern': r'^[A-Z0-9]+_\d+\.\d+_(.+)_(?:rbd|spike)$',
                }
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
                "ref": r[8]
            }
        )

    return out_data