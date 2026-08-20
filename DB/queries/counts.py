from typing import List, Any, Dict

from sqlalchemy import text, Result

from DB.engine import get_async_session
from DB.queries.helpers import get_ih_table_and_change_cols
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa, ColumnNames, COLLECTION_DATE


async def count_samples_by_column(by_col: str, where: str | None = None):
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where {parser.parse(where)}'

    query = f'''
        select {by_col}, count(*) as count1
        from samples s
        left join geo_locations gl on gl.id = s.geo_location_id
        {user_where_clause}
        group by {by_col}
        order by count1 desc
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        return await _package_count_by_column(res)


async def count_variants_by_column(
    by_col: str,
    change_bin: NtOrAa = NtOrAa.aa,
    where: str | None = None
) -> Dict[str, int]:
    ih_table, change_id_col, catalog_table, *_ = get_ih_table_and_change_cols(change_bin)

    # Counts are of (sample, change) observations. The frequency bins of a single change are collapsed
    # with rb_or_agg *before* counting, so a sample that appears in two bins for the same change is
    # counted once; summing rb_cardinality per bin would count it twice.
    if where is None:
        subset_cte = ''
        per_change_count = f'rb_or_cardinality_agg(v.{ColumnNames.samples_present})'
        having_clause = ''
    else:
        subset_cte = f'''
            with sample_subset_bm as (
                select coalesce(rb_build_agg(s.id), rb_build('{{}}')) as bm
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                left join samples_lineages sl on sl.sample_id = s.id
                left join lineages l on l.id = sl.lineage_id
                left join lineage_systems ls on ls.id = l.lineage_system_id
                where {parser.parse(where)}
            )
        '''
        per_change_count = \
            f'rb_and_cardinality(rb_or_agg(v.{ColumnNames.samples_present}), (select bm from sample_subset_bm))'
        having_clause = 'having sum(n) > 0'

    # the group key is cast to text in the database: group_by=alt_freq_range is a numrange, and
    # str()-ing the driver's Range object client-side gives "<Range [Decimal('0'), ...]>" as the key
    query = f'''
        {subset_cte}
        select {by_col}::text, sum(n)::bigint as count1
        from (
            select {by_col}, {per_change_count} as n
            from {ih_table} v
            inner join {catalog_table} t on t.id = v.{change_id_col}
            group by v.{change_id_col}, {by_col}
        )
        group by {by_col}
        {having_clause}
        order by count1 desc
    '''

    async with get_async_session() as session:
        res = await session.execute(text(query))
        return await _package_count_by_column(res)


async def count_mutations_by_column(by_col: str, change_bin: NtOrAa = NtOrAa.aa, where: str | None = None):
    if change_bin == NtOrAa.nt:
        cns_table, join_table, join_key = 'cns_samples_by_allele', 'alleles', 'allele_id'
        transposed_table, present_col = 'cns_alleles_by_sample', 'alleles_present'
    else:
        cns_table, join_table, join_key = 'cns_samples_by_amino_acid', 'amino_acids', 'amino_acid_id'
        transposed_table, present_col = 'cns_amino_acids_by_sample', 'amino_acids_present'

    if where is None:
        query = f'''
            select {by_col}, sum(rb_cardinality(m.samples_present))::bigint as count1
            from {cns_table} m
            inner join {join_table} t on t.id = m.{join_key}
            group by {by_col}
            order by count1 desc
            '''
    else:
        query = f'''
            select {by_col}, count(*)::bigint as count1
            from {transposed_table} cs
            cross join lateral unnest(rb_to_array(cs.{present_col})) as u({join_key})
            inner join {join_table} t on t.id = u.{join_key}
            where cs.sample_id in (
                select s.id
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                where {parser.parse(where)}
            )
            group by {by_col}
            order by count1 desc
            '''

    async with get_async_session() as session:
        res = await session.execute(text(query))
        return await _package_count_by_column(res)


async def _package_count_by_column(query_result: Result[tuple[Any, int]] | List[tuple]) -> Dict[str, int]:
    return {str(r[0]): r[1] for r in query_result}


async def count_samples_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    days: int | None,
    where: str | None
):
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where {parser.parse(where)}'

    extract_clause = get_extract_clause(group_by, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(*)
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                {user_where_clause}
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data[date] = r[2]
    return out_data


async def count_samples_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    where: str | None,
    max_span_days: int,
) -> Dict[str, int]:
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where {parser.parse(where)}'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                    {extract_clause},
                    count(*)
                from (
                    select
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                        *,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        {user_where_clause}
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
        out_data[date] = r[2]
    return out_data


async def count_variants_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    where: str | None = None
) -> Dict[str, Dict[str, int]]:
    ih_table, change_id_col, catalog_table, feature_col, ref_col, pos_col, alt_col = \
        get_ih_table_and_change_cols(change_bin)

    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and ({parser.parse(where)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin, [feature_col, ref_col, pos_col, alt_col])
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with sample_subset as (
                    select distinct
                        s.id as sample_id,
                        s.collection_start_date,
                        s.collection_end_date
                    from samples s
                    left join geo_locations gl on gl.id = s.geo_location_id
                    left join samples_lineages sl on sl.sample_id = s.id
                    left join lineages l on l.id = sl.lineage_id
                    left join lineage_systems ls on ls.id = l.lineage_system_id
                    where num_nulls(s.collection_end_date, s.collection_start_date) = 0
                        and s.collection_end_date - s.collection_start_date <= {max_span_days}
                        {user_where_clause}
                ),
                sample_subset_bm as (
                    select coalesce(rb_build_agg(sample_id), rb_build('{{}}')) as bm
                    from sample_subset
                )
                select
                {extract_clause},
                count(distinct sample_id),
                {feature_col}, {ref_col}, {pos_col}, {alt_col}
                from (
                    select
                        ss.sample_id,
                        c.{feature_col}, c.{ref_col}, c.{pos_col}, c.{alt_col},
                        {MID_COLLECTION_DATE_CALCULATION}
                    from {ih_table} v
                    inner join {catalog_table} c on c.id = v.{change_id_col}
                    cross join lateral unnest(
                        rb_to_array(v.{ColumnNames.samples_present} & (select bm from sample_subset_bm))
                    ) as u(sample_id)
                    inner join sample_subset ss on ss.sample_id = u.sample_id
                )
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[2]
        feature = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{feature}:{ref}{pos}{alt}'
        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


async def count_mutations_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    where: str | None = None
):
    if change_bin == NtOrAa.nt:
        transposed_table, present_col, join_table, join_key = \
            'cns_alleles_by_sample', 'alleles_present', 'alleles', 'allele_id'
        feature_col, ref_col, pos_col, alt_col = 'region', 'ref_nt', 'position_nt', 'alt_nt'
    else:
        transposed_table, present_col, join_table, join_key = \
            'cns_amino_acids_by_sample', 'amino_acids_present', 'amino_acids', 'amino_acid_id'
        feature_col, ref_col, pos_col, alt_col = 'gff_feature', 'ref_aa', 'position_aa', 'alt_aa'

    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and ({parser.parse(where)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin, [feature_col, ref_col, pos_col, alt_col])
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(distinct sample_id),
                {feature_col}, {ref_col}, {pos_col}, {alt_col}
                from (
                    select
                    *,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                            s.id as sample_id,
                            c.{feature_col}, c.{ref_col}, c.{pos_col}, c.{alt_col},
                            s.collection_start_date, s.collection_end_date,
                            s.collection_end_date - s.collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        left join samples_lineages sl on sl.sample_id = s.id
                        left join lineages l on l.id = sl.lineage_id
                        left join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join {transposed_table} t on t.sample_id = s.id
                        cross join lateral unnest(rb_to_array(t.{present_col})) as u({join_key})
                        inner join {join_table} c on c.id = u.{join_key}
                        where num_nulls(s.collection_end_date, s.collection_start_date) = 0 {user_where_clause}
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
        count = r[2]
        feature = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{feature}:{ref}{pos}{alt}'
        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


async def count_lineages_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    where: str | None,
    days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where {parser.parse(where)}'

    extract_clause = get_extract_clause(group_by, date_bin, days)
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
                count(*)
                from (
                        select
                        {group_by},
                        lineage_name,
                        lineage_system_name
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join samples s on s.id = sl.sample_id
                        left join geo_locations gl on gl.id = s.geo_location_id
                        {user_where_clause}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[4]
        lineage = r[2]
        system = r[3]

        try:
            out_data[date][system][lineage] = count
        except KeyError:
            if date not in out_data.keys():
                out_data[date] = {system: {lineage: count}}
            elif system not in out_data[date].keys():
                out_data[date][system] = {lineage: count}
    return out_data


async def count_lineages_by_collection_date(
    date_bin: DateBinOpt,
    where: str | None,
    days: int,
    max_span_days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and {parser.parse(where)}'

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
               with lin_samp_date as (
                    select lineage_name,
                           lineage_system_name,
                           collection_start_date,
                           collection_end_date,
                           collection_end_date - collection_start_date as collection_span,
                           {MID_COLLECTION_DATE_CALCULATION}
                    from samples_lineages sl
                    inner join lineages l on l.id = sl.lineage_id
                    inner join lineage_systems ls on ls.id = l.lineage_system_id
                    inner join samples s on s.id = sl.sample_id
                    left join geo_locations gl on gl.id = s.geo_location_id
                    where collection_end_date - collection_start_date <= {max_span_days} {user_where_clause}
                )
                select {extract_clause},
                       lineage_name,
                       lineage_system_name,
                       count(*)
                from lin_samp_date
                {group_by_clause}
                {order_by_clause};
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[4]
        lineage = r[2]
        system = r[3]

        try:
            out_data[date][system][lineage] = count
        except KeyError:
            if date not in out_data.keys():
                out_data[date] = {system: {lineage: count}}
            elif system not in out_data[date].keys():
                out_data[date][system] = {lineage: count}
    return out_data
