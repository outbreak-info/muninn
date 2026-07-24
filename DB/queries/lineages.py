from collections import defaultdict
from typing import List, Dict

from sqlalchemy import text

from DB.engine import get_async_session
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    YEAR, CHUNK, BIN_START, BIN_END, MID_COLLECTION_DATE_CALCULATION
from api.models import LineageCountInfo, LineageAbundanceInfo, LineageInfo, LineageAbundanceSummaryInfo, \
    MutationProfileInfo, LineageCountWithPrevalenceInfo, MutationProfileWithPrevalenceInfo
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa, NUCLEOTIDE_CHARACTERS, TableNames, ColumnNames, COLLECTION_DATE
from utils.errors import NotFoundError


async def get_all_lineages_by_lineage_system(lineage_system_name: str) -> List[LineageInfo]:
    query = f'''
        select
            l.id as lineage_id,
            l.{ColumnNames.lineage_name} as lineage_name,
            ls.id as lineage_system_id,
            ls.{ColumnNames.lineage_system_name} as lineage_system_name
        from {TableNames.lineages} l
        inner join {TableNames.lineage_systems} ls on l.{ColumnNames.lineage_system_id} = ls.id
        where ls.{ColumnNames.lineage_system_name} = :lineage_system_name
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query), {'lineage_system_name': lineage_system_name})
        out_data = [LineageInfo(**row) for row in res.mappings().all()]
    return out_data


async def get_sample_counts_by_lineage(filter: str | None) -> List[LineageCountInfo]:
    where_clause = ''
    if filter is not None:
        user_defined_filter = parser.parse(filter)
        where_clause = f'''
            where sl.{ColumnNames.sample_id} in (
                select s.id
                from {TableNames.samples} s
                left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                where {user_defined_filter}
            )
        '''

    query = f'''
        select
            ls.{ColumnNames.lineage_system_name} as lineage_system_name,
            l.{ColumnNames.lineage_name} as lineage_name,
            count(distinct sl.{ColumnNames.sample_id}) as count1
        from {TableNames.samples_lineages} sl
        left join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
        left join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        {where_clause}
        group by ls.{ColumnNames.lineage_system_name}, l.{ColumnNames.lineage_name}
        order by count1 desc
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        out_data = [
            LineageCountInfo(
                count=row['count1'],
                lineage=row['lineage_name'],
                lineage_system=row['lineage_system_name'],
            )
            for row in res.mappings().all()
        ]
    return out_data


async def get_abundances(filter: str | None) -> List[LineageAbundanceInfo]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    query = f'''
        select
            sl.{ColumnNames.lineage_id} as lineage_id,
            l.{ColumnNames.lineage_name} as lineage_name,
            ls.{ColumnNames.lineage_system_name} as lineage_system_name,
            l.{ColumnNames.lineage_system_id} as lineage_system_id,
            sl.{ColumnNames.sample_id} as sample_id,
            s.{ColumnNames.accession} as accession,
            sl.{ColumnNames.abundance} as abundance
        from {TableNames.samples_lineages} sl
        inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        inner join {TableNames.samples} s on s.id = sl.{ColumnNames.sample_id}
        left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
        where sl.{ColumnNames.is_consensus_call} = false {user_defined_filter}
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        out_data = [
            LineageAbundanceInfo(
                lineage_info=LineageInfo(
                    lineage_id=row['lineage_id'],
                    lineage_name=row['lineage_name'],
                    lineage_system_name=row['lineage_system_name'],
                    lineage_system_id=row['lineage_system_id'],
                ),
                sample_id=row['sample_id'],
                accession=row['accession'],
                abundance=row['abundance'],
            )
            for row in res.mappings().all()
        ]
    return out_data


async def get_abundance_summaries(filter: str | None) -> List[LineageAbundanceSummaryInfo]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    query = f'''
        select
            l.{ColumnNames.lineage_name} as lineage_name,
            ls.{ColumnNames.lineage_system_name} as lineage_system_name,
            count(*) as sample_count,
            min(sl.{ColumnNames.abundance}) as abundance_min,
            percentile_cont(0.25) within group (order by sl.{ColumnNames.abundance}) as abundance_q1,
            percentile_cont(0.5) within group (order by sl.{ColumnNames.abundance}) as abundance_median,
            percentile_cont(0.75) within group (order by sl.{ColumnNames.abundance}) as abundance_q3,
            max(sl.{ColumnNames.abundance}) as abundance_max
        from {TableNames.samples_lineages} sl
        inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        inner join {TableNames.samples} s on s.id = sl.{ColumnNames.sample_id}
        left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
        where sl.{ColumnNames.is_consensus_call} = false {user_defined_filter}
        group by l.{ColumnNames.lineage_name}, ls.{ColumnNames.lineage_system_name}
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        out_data = [LineageAbundanceSummaryInfo(**row) for row in res.mappings().all()]
    return out_data


async def get_abundance_summaries_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    filter: str | None,
    max_span_days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [ColumnNames.lineage_name, ColumnNames.lineage_system_name]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                lineage_name,
                lineage_system_name,
                count(*) as samp_count,
                min(abundance) as min,
                percentile_cont(0.25) within group (order by abundance) as q1,
                percentile_cont(0.5) within group (order by abundance) as median,
                percentile_cont(0.75) within group (order by abundance) as q3,
                max(abundance) as max
                from(
                    select
                    *,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from(
                        select
                        l.{ColumnNames.lineage_name},
                        ls.{ColumnNames.lineage_system_name},
                        sl.{ColumnNames.abundance},
                        collection_start_date,
                        collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from {TableNames.samples_lineages} sl
                        inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
                        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
                        inner join {TableNames.samples} s on s.id = sl.{ColumnNames.sample_id}
                        left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                        where sl.{ColumnNames.is_consensus_call} = false {user_defined_filter}

                    )
                    where collection_span <= {max_span_days}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        info = LineageAbundanceSummaryInfo(
            lineage_name=r[2],
            lineage_system_name=r[3],
            sample_count=r[4],
            abundance_min=r[5],
            abundance_q1=r[6],
            abundance_median=r[7],
            abundance_q3=r[8],
            abundance_max=r[9]
        )
        try:
            out_data[date].append(info)
        except KeyError:
            out_data[date] = [info]
    return out_data


async def get_lineage_counts_over_time(
    date_bin: DateBinOpt,
    days: int,
    filter: str | None,
    max_span_days: int,
    days_before_today: int | None = None,
    lineage: str | None = None,
) -> Dict[str, List[LineageCountWithPrevalenceInfo]]:
    """
    Per-collection-date-bin sample counts per lineage over consensus lineage calls only, optionally
    restricted to a single lineage and/or to samples whose collection midpoint is within the last
    `days_before_today` days.
    """
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    query_params = {}
    lineage_clause = ''
    if lineage is not None:
        lineage_clause = f'where {ColumnNames.lineage_name} = :input_lineage'
        query_params['input_lineage'] = lineage

    # Recency window: keep bins whose collection midpoint is no older than `days_before_today` days.
    # mid_collection_date is a column of the CTE, so this filters before the group-by.
    recency_clause = ''
    if days_before_today is not None:
        recency_clause = f'where mid_collection_date >= current_date - {days_before_today}'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [ColumnNames.lineage_name, ColumnNames.lineage_system_name]
    )
    order_by_clause = get_order_by_cause(date_bin)

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            bin_cols = f'{YEAR}, {CHUNK}'
        case DateBinOpt.day:
            bin_cols = f'{BIN_END}, {BIN_START}'
        case _:
            raise NotImplementedError

    query = f'''
        with with_mid_date as (
            select
                l.{ColumnNames.lineage_name},
                ls.{ColumnNames.lineage_system_name},
                {MID_COLLECTION_DATE_CALCULATION}
            from {TableNames.samples_lineages} sl
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            inner join {TableNames.samples} s on s.id = sl.{ColumnNames.sample_id}
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            where sl.{ColumnNames.is_consensus_call} = true
                  and collection_end_date - collection_start_date <= {max_span_days}
                  {user_defined_filter}
        ),
        binned as (
            select
                {extract_clause},
                {ColumnNames.lineage_name},
                {ColumnNames.lineage_system_name},
                count(*) as cnt
            from with_mid_date
            {recency_clause}
            {group_by_clause}
        ),
        with_total as (
            select
                {bin_cols},
                {ColumnNames.lineage_name},
                {ColumnNames.lineage_system_name},
                cnt,
                sum(cnt) over (partition by {bin_cols}) as total
            from binned
        )
        select
            {bin_cols},
            {ColumnNames.lineage_name},
            {ColumnNames.lineage_system_name},
            cnt,
            total
        from with_total
        {lineage_clause}
        {order_by_clause}, cnt desc
    '''

    async with get_async_session() as session:
        res = await session.execute(text(query), query_params)

    out_data: Dict[str, List[LineageCountWithPrevalenceInfo]] = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        total = r[5]
        info = LineageCountWithPrevalenceInfo(
            count=r[4],
            lineage=r[2],
            lineage_system=r[3],
            total=total,
            prevalence=(r[4] / total) if total else 0.0,
        )
        out_data.setdefault(date, []).append(info)
    return out_data


async def get_mutation_incidence(
    lineage: str,
    lineage_system_name: str,
    change_bin: NtOrAa,
    prevalence_threshold: float,
    match_reference: bool,
    filter: str | None
) -> Dict:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    query_params = {
        'input_lineage': lineage,
        'input_lineage_system_name': lineage_system_name
    }

    async with get_async_session() as session:
        sample_count = await session.scalar(
            text(
                f'select count(distinct s.id)\n'
                f'from {TableNames.samples} s\n'
                f'left join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id\n'
                f'left join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}\n'
                f'left join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id} \n'
                f'WHERE l.{ColumnNames.lineage_name} = :input_lineage and ls.{ColumnNames.lineage_system_name} = :input_lineage_system_name\n'
                f'{user_defined_filter}'
            ), query_params
        )

        if not sample_count:
            return {'sample_count': 0, 'mutation_counts': {}}

        sample_subset_query = f"""
        select s.id as sample_id from {TableNames.samples} s
        inner join {TableNames.samples_lineages} sl ON sl.{ColumnNames.sample_id} = s.id
        inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        where l.{ColumnNames.lineage_name} = :input_lineage and ls.{ColumnNames.lineage_system_name} = :input_lineage_system_name
        {user_defined_filter}
        """

        if change_bin == NtOrAa.nt:
            not_reference = f'where {ColumnNames.ref_nt} <> {ColumnNames.alt_nt}'
            if match_reference:
                not_reference = ''

            res = await session.execute(
                text(f'''
                with sample_subset as (
                    {sample_subset_query}
                ),
                sample_subset_bm as (
                    select rb_build_agg(sample_id) as bm from sample_subset
                ),
                counted as (
                    select  a.{ColumnNames.ref_nt},
                            a.{ColumnNames.position_nt},
                            a.{ColumnNames.alt_nt},
                            a.{ColumnNames.region},
                            rb_and_cardinality(m.{ColumnNames.samples_present}, (select bm from sample_subset_bm)) as mutation_count
                    from {TableNames.cns_samples_by_allele} m
                    inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
                    {not_reference}
                )
                select *,
                        mutation_count / {sample_count}::decimal as mutation_prevalence
                from counted
                where mutation_count >= {prevalence_threshold} * {sample_count}
                '''
                ),
                query_params
            )
        else:
            not_reference = f'where {ColumnNames.ref_aa} <> {ColumnNames.alt_aa}'
            if match_reference:
                not_reference = ''
            res = await session.execute(
                text(f'''
                with sample_subset as (
                    {sample_subset_query}
                ),
                sample_subset_bm as (
                    select rb_build_agg(sample_id) as bm from sample_subset
                ),
                counted as (
                    select  aa.{ColumnNames.ref_aa},
                            aa.{ColumnNames.position_aa},
                            aa.{ColumnNames.alt_aa},
                            aa.{ColumnNames.gff_feature},
                            rb_and_cardinality(m.{ColumnNames.samples_present}, (select bm from sample_subset_bm)) as mutation_count
                    from {TableNames.cns_samples_by_amino_acid} m
                    inner join {TableNames.amino_acids} aa on aa.id = m.{ColumnNames.amino_acid_id}
                    {not_reference}
                )
                select *,
                        mutation_count / {sample_count}::decimal as mutation_prevalence
                from counted
                where mutation_count >= {prevalence_threshold} * {sample_count}
                '''
                ),
                query_params
            )
    out = defaultdict(list)
    for ref, pos, alt, region_or_gff, count, prevalence in res:
        out[region_or_gff].append({"ref": ref, "alt": alt, "pos": pos, "count": count, "prevalence": prevalence})
    return {'sample_count': sample_count, 'mutation_counts': out}


async def get_mutation_profile(
    lineage: str,
    lineage_system_name: str,
    filter: str | None
) -> List['MutationProfileWithPrevalenceInfo']:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    nt_list = ', '.join(f"'{c}'" for c in NUCLEOTIDE_CHARACTERS)

    query = f'''
        with sample_subset as (
            select s.id as sample_id from {TableNames.samples} s
            inner join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            where l.{ColumnNames.lineage_name} = :input_lineage
                  and ls.{ColumnNames.lineage_system_name} = :input_lineage_system_name
                  {user_defined_filter}
        ),
        sample_subset_bm as (
            select rb_build_agg(sample_id) as bm from sample_subset
        ),
        per as (
            select a.{ColumnNames.region} as region,
                   a.{ColumnNames.ref_nt} as ref_nt,
                   a.{ColumnNames.alt_nt} as alt_nt,
                   rb_and_cardinality(m.{ColumnNames.samples_present}, (select bm from sample_subset_bm)) as card
            from {TableNames.cns_samples_by_allele} m
            inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
            where a.{ColumnNames.ref_nt} in ({nt_list}) and a.{ColumnNames.alt_nt} in ({nt_list})
        ),
        grouped as (
            select region, ref_nt, alt_nt, sum(card) as count
            from per
            group by region, ref_nt, alt_nt
            having sum(card) > 0
        )
        select
            region, ref_nt, alt_nt, count,
            sum(count) over (partition by region) as total,
            count::float / nullif(sum(count) over (partition by region), 0) as prevalence
        from grouped
    '''
    async with get_async_session() as session:
        results = await session.execute(
            text(query),
            {'input_lineage': lineage, 'input_lineage_system_name': lineage_system_name}
        )
        out_data = [MutationProfileWithPrevalenceInfo(**row) for row in results.mappings().all()]
    return out_data




async def get_mutation_incidence_from_cache(
    lineage: str,
    lineage_system_name: str,
    prevalence_threshold: float,
    match_reference: bool,
):
    reference_filter = 'and a.ref_nt <> a.alt_nt'
    if match_reference:
        reference_filter = ''

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'select a.region,\n'
                f'       a.ref_nt,\n'
                f'       a.position_nt,\n'
                f'       a.alt_nt,\n'
                f'       incidence,\n'
                f'       n_samples,\n'
                f'       incidence / n_samples::decimal as prevalence\n'
                f'from lineages l\n'
                f'inner join lineage_systems ls on ls.id = l.lineage_system_id\n'
                f'inner join cache_allele_prevalence_by_lineage cache on cache.lineage_id = l.id\n'
                f'inner join alleles a on a.id = cache.allele_id\n'
                f'where l.lineage_name = :input_lineage_name '
                f'      and ls.lineage_system_name = :input_system_name\n'
                f'      and incidence / n_samples::decimal >= :input_threshold\n'
                f'      {reference_filter}'
            ),
            {
                'input_system_name': lineage_system_name,
                'input_lineage_name': lineage,
                'input_threshold': prevalence_threshold
            }
        )

    rows = res.all()
    if len(rows) == 0:
        raise NotFoundError(f'Not found in cache: {lineage_system_name} {lineage}')

    out = defaultdict(list)
    sample_count = None
    for region, ref, pos, alt, incidence, n_samples, prevalence in rows:
        out[region].append({"ref": ref, "alt": alt, "pos": pos, "count": incidence, "prevalence": prevalence})
        if sample_count is not None and n_samples != sample_count:
            raise ValueError('Mismatch in n_samples in response from cached mutation incidence by lineage.')
        elif sample_count is None:
            sample_count = n_samples
    return {'sample_count': sample_count, 'mutation_counts': out}
