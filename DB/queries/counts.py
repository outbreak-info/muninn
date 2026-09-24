from typing import List, Any, Dict

from sqlalchemy import Result

from DB.engine import get_async_session
from DB.textutils import text
from DB.queries.helpers import get_ih_table_and_change_cols
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION, get_date_column_names
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa, ColumnNames, COLLECTION_DATE, TableNames


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

    if where is None:
        subset_cte = ''
        per_row_count = f'rb_cardinality(v.{ColumnNames.samples_present})'
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
        per_row_count = f'rb_and_cardinality(v.{ColumnNames.samples_present}, (select bm from sample_subset_bm))'
        having_clause = 'having sum(n) > 0'

    query = f'''
        {subset_cte}
        select {by_col}::text, sum(n)::bigint as count1
        from (
            select {by_col}, {per_row_count} as n
            from {ih_table} v
            inner join {catalog_table} t on t.id = v.{change_id_col}
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
        cns_table, join_table, join_key = TableNames.cns_samples_by_allele, TableNames.alleles, ColumnNames.allele_id
    else:
        cns_table, join_table, join_key = TableNames.cns_samples_by_amino_acid, TableNames.amino_acids, ColumnNames.amino_acid_id

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
                with matching_samples as (
                    select s.id
                    from samples s
                    left join geo_locations gl on gl.id = s.geo_location_id
                    where {parser.parse(where)}
                ),
                samples_bm as (
                    select rb_build_agg(id) as bitmap from matching_samples
                )
                select {by_col},
                       sum(rb_cardinality(samples_bm.bitmap & CNS.{ColumnNames.samples_present}))::bigint as count1
                from {cns_table} CNS
                inner join {join_table} JT on JT.id = CNS.{join_key}
                cross join samples_bm
                group by {by_col}
                order by count1 desc;
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
                left join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
                left join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
                left join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
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
                        left join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
                        left join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
                        left join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
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
                count(*),
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
    where: str | None = None,
    mutations_where: str | None = None
):
    if change_bin == NtOrAa.nt:
        consensus_table = TableNames.cns_samples_by_allele
        join_table = TableNames.alleles
        join_key = ColumnNames.allele_id
        feature_col = ColumnNames.region
        ref_col = ColumnNames.ref_nt
        pos_col = ColumnNames.position_nt
        alt_col = ColumnNames.alt_nt

    else:
        consensus_table = TableNames.cns_samples_by_amino_acid
        join_table = TableNames.amino_acids
        join_key = ColumnNames.amino_acid_id
        feature_col = ColumnNames.gff_feature
        ref_col = ColumnNames.ref_aa
        pos_col = ColumnNames.position_aa
        alt_col = ColumnNames.alt_aa

    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and ({parser.parse(where)})'

    mutations_where_clause = ''
    if mutations_where is not None:
        mutations_where_clause = f'where ({parser.parse(mutations_where)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin, [feature_col, ref_col, pos_col, alt_col])
    group_by_clause_date_only = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    query = f'''
            with matching_samples as (
                select s.id as sample_id,
                       {MID_COLLECTION_DATE_CALCULATION}
                from samples s
                left join {TableNames.geo_locations} gl on gl.id = s.geo_location_id
                left join {TableNames.samples_lineages} sl on sl.sample_id = s.id
                left join {TableNames.lineages} l on l.id = sl.lineage_id
                left join {TableNames.lineage_systems} ls on ls.id = l.lineage_system_id
                where num_nulls(s.{ColumnNames.collection_end_date}, s.{ColumnNames.collection_start_date}) = 0
                  and s.{ColumnNames.collection_end_date} - s.{ColumnNames.collection_start_date} <= :max_span_days
                  {user_where_clause}
            ),
            samps_dated_bm as (
                select rb_build_agg(sample_id) as samples_present,
                       {extract_clause}
                from matching_samples
                {group_by_clause_date_only}
            )
            select {get_date_column_names(date_bin)},
                   sum(rb_cardinality(samps_dated_bm.samples_present & CNS.{ColumnNames.samples_present})) as count,
                   J.{feature_col},
                   J.{ref_col},
                   J.{pos_col},
                   J.{alt_col}
            from {consensus_table} CNS
            inner join {join_table} J on J.id = CNS.{join_key}
            inner join samps_dated_bm on samps_dated_bm.samples_present && CNS.{ColumnNames.samples_present}
            {mutations_where_clause}
            {group_by_clause}
            {order_by_clause}
            '''

    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'max_span_days': max_span_days
            }
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
