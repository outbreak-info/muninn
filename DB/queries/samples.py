from typing import List, Dict

from DB.textutils import text

from DB.engine import get_async_session
from api.models import SampleInfo, LineageCountInfo
from parser.parser import parser
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from DB.queries.counts import count_samples_by_column, count_samples_by_simple_date, \
    count_samples_by_collection_date, count_samples_by_lineage_and_simple_date, \
    count_samples_by_lineage_and_collection_date
from DB.queries.lineages import get_sample_counts_by_lineage
from DB.queries.helpers import get_ih_table_and_change_cols
from utils.constants import DateBinOpt, NtOrAa, TableNames, ColumnNames, COLLECTION_DATE, DEFAULT_DAYS, \
    DEFAULT_MAX_SPAN_DAYS, LINEAGE, SIMPLE_DATE_FIELDS


async def get_sample_by_id(sample_id: int) -> SampleInfo | None:
    query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id = :sample_id
    """

    async with get_async_session() as session:
        result = await session.execute(text(query), {'sample_id': sample_id})
        row = result.mappings().first()
    if row is None:
        return None
    return SampleInfo(**row)


async def get_samples(where: str) -> List['SampleInfo']:
    user_where_clause = parser.parse(where)

    query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where {user_where_clause}
    """

    async with get_async_session() as session:
        samples = await session.execute(text(query))
        out_data = [SampleInfo(**row) for row in samples.mappings().all()]
    return out_data


async def get_samples_by_mutation(change_bin: NtOrAa = NtOrAa.aa, where: str = "") -> List['SampleInfo']:
    user_where_clause = parser.parse(where)

    if change_bin == NtOrAa.nt:
        matching_samples = f'''
            select samps.sample_id
            from {TableNames.cns_samples_by_allele} m
            inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
            cross join lateral unnest(rb_to_array(m.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''
    else:
        matching_samples = f'''
            select samps.sample_id
            from {TableNames.cns_samples_by_amino_acid} mt
            inner join {TableNames.amino_acids} aa on aa.id = mt.{ColumnNames.amino_acid_id}
            cross join lateral unnest(rb_to_array(mt.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''

    samples_query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id in (
        {matching_samples}
    )
    """

    async with get_async_session() as session:
        samples = await session.execute(text(samples_query))
        out_data = [SampleInfo(**row) for row in samples.mappings().all()]
    return out_data


async def get_samples_by_variant(
    change_bin: NtOrAa = NtOrAa.aa,
    where: str = "",
    min_alt_freq: float | None = None,
    max_alt_freq: float | None = None
) -> List['SampleInfo']:
    user_where_clause = parser.parse(where)
    ih_table, change_id_col, catalog_table, *_ = get_ih_table_and_change_cols(change_bin)

    samples_query = f"""
    with matching_bm as (
        select rb_or_agg(v.{ColumnNames.samples_present}) as bm
        from {ih_table} v
        inner join {catalog_table} c on c.id = v.{change_id_col}
        where {user_where_clause}
        and v.alt_freq_range && numrange(:min_alt_freq, :max_alt_freq, '[]')
    )
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id = any(rb_to_array((select bm from matching_bm)))
    """

    async with get_async_session() as session:
        samples = await session.execute(
            text(samples_query),
            {'min_alt_freq': min_alt_freq, 'max_alt_freq': max_alt_freq}
        )
        return [SampleInfo(**row) for row in samples.mappings().all()]


async def get_sample_counts(
    group_by: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    where: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
) -> Dict[str, int] | Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo]:
    group_by_set = set(group_by.split(','))
    if len(group_by_set) > 1:
        if len(group_by_set) > 2:
            raise ValueError('Max of 2 group_by values allowed')
        if LINEAGE in group_by_set:
            date_field = group_by_set.difference({LINEAGE}).pop()
            if date_field in SIMPLE_DATE_FIELDS:
                return await count_samples_by_lineage_and_simple_date(date_field, date_bin, where, days)
            elif date_field == COLLECTION_DATE:
                return await count_samples_by_lineage_and_collection_date(date_bin, where, days, max_span_days)

        raise NotImplementedError(
            'Grouping by multiple fields is currently only supported for lineage plus a date field'
        )
    else:
        if group_by in SIMPLE_DATE_FIELDS:
            return await count_samples_by_simple_date(group_by, date_bin, days, where)
        elif group_by == COLLECTION_DATE:
            return await count_samples_by_collection_date(date_bin, days, where, max_span_days)
        elif group_by == LINEAGE:
            return await get_sample_counts_by_lineage(where)
        else:
            return await count_samples_by_column(group_by, where)


async def get_sample_collection_release_lag(
    max_span_days: int,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
) -> List[Dict]:
    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    query = f"""
    select
        {extract_clause},
        percentile_cont(0.25) within group (order by lag) as q1,
        percentile_cont(0.5)  within group (order by lag) as median,
        percentile_cont(0.75) within group (order by lag) as q3
    from (
        select
            mid_collection_date,
            {ColumnNames.release_date}::date - mid_collection_date as lag
        from (
            select
                {MID_COLLECTION_DATE_CALCULATION},
                {ColumnNames.release_date}
            from {TableNames.samples}
            where ({ColumnNames.collection_end_date} - {ColumnNames.collection_start_date}) <= :max_span_days
        ) midpoints
    ) lagged
    {group_by_clause}
    {order_by_clause}
    """

    async with get_async_session() as session:
        result = await session.execute(text(query), {'max_span_days': max_span_days})
        rows = result.all()
    return [
        {
            "collection_date_bin": date_bin.format_iso_chunk(row[0], row[1]),
            "lag_q1": row.q1,
            "lag_median": row.median,
            "lag_q3": row.q3
        }
        for row in rows
    ]


async def get_sample_counts_for_lineage(
    group_by: str,
    lineage_name: str,
    lineage_system_name: str,
    where: str,
) -> list[dict[str, str]]:
    if lineage_name is None:
        raise ValueError('Lineage name is required')


    group_by_set = set()
    if group_by is not None:
        group_by_set = set(group_by.split(','))
    for gb in group_by_set:
        ColumnNames.assert_name_exists(gb)

    # "simple" as opposed to date columns, which we might want to add at some point
    simple_group_by_cols = ','.join(group_by_set)

    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where {parser.parse(where)}'

    include_descendants = lineage_name.endswith('*')
    lineage_name.rstrip('*')

    lineage_system_where = ''
    if lineage_system_name is not None:
        lineage_system_where = f'and lineage_system_name = :lineage_system_name'

    descendants_union = ''
    if include_descendants:
        descendants_union = f'''
                            union all
                            select {ColumnNames.child_id} as lineage_id
                            from {TableNames.lineages_deep_children} ldc
                            inner join {TableNames.lineages} l on l.id = ldc.{ColumnNames.parent_id}
                            where l.{ColumnNames.lineage_name} = :lineage_name
                                  {lineage_system_where}
                            '''

    query = f'''
            with matching_samples as (
                select s.id as sample_id,
                       {simple_group_by_cols}
                from {TableNames.samples} s
                left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                {user_where_clause}
            ),
            total_samples as (
                select count(distinct sample_id) as n_date_location_samples,
                       {simple_group_by_cols}
                from matching_samples
                group by {simple_group_by_cols}
            ),
            lineage_and_descendants as (
                select id as lineage_id
                from {TableNames.lineages}
                where {ColumnNames.lineage_name} = :lineage_name
                      {lineage_system_where}
                {descendants_union}
            ),
            lineage_samples as (
                select count(distinct sample_id) as n_lineage_samples,
                       {simple_group_by_cols},
                       {ColumnNames.lineage_name},
                       {ColumnNames.lineage_system_name}
                from matching_samples
                inner join {TableNames.samples_lineages} sl using (sample_id)
                inner join lineage_and_descendants using (lineage_id)
                inner join {TableNames.lineages} l on l.id = lineage_id
                inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
                group by {simple_group_by_cols}, {ColumnNames.lineage_name}, {ColumnNames.lineage_system_name}
            )
            select {simple_group_by_cols}
                   {ColumnNames.lineage_name},
                   {ColumnNames.lineage_system_name},
                   n_lineage_samples,
                   n_total_samples
            from lineage_samples
            inner join total_samples using ({simple_group_by_cols});
            '''

    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'lineage_name': lineage_name,
                'lineage_system_name': lineage_system_name
            }
        )

    out = []
    for r in res.mappings():
        o = {
            {ColumnNames.lineage_name}: r[{ColumnNames.lineage_name}],
            {ColumnNames.lineage_system_name}: r[{ColumnNames.lineage_system_name}],
            'n_lineage_samples': r['n_lineage_samples'],
            'n_total_samples': r['n_total_samples']
        }
        for gb in simple_group_by_cols:
            o[gb] = r[gb]
        out.append(o)
