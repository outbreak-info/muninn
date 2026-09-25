from datetime import date
from typing import List, Any, Dict

from sqlalchemy import Result

from DB.engine import get_async_session
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, \
    get_date_column_names, get_order_by_cause, MID_COLLECTION_DATE_CALCULATION, YEAR, CHUNK
from DB.textutils import text
from api.models import LineageAbundanceWithSampleInfo, AverageLineageAbundanceInfo, SampleInfo
from parser.parser import parser
from utils.constants import DEFAULT_MAX_SPAN_DAYS, ColumnNames, TableNames, DateBinOpt, COLLECTION_DATE


async def get_lineage_abundances_by_sample(
    where: str | None,
) -> List[LineageAbundanceWithSampleInfo]:
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and ({parser.parse(where)})'

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                    s.accession,
                    gl.admin1_name,
                    s.ww_collected_by,
                    s.ww_site_id,
                    l.lineage_name,
                    sl.abundance,
                    s.ww_viral_load,
                    s.ww_catchment_population,
                    s.collection_start_date
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id
                inner join samples s on s.id = sl.sample_id
                inner join geo_locations gl on gl.id = s.geo_location_id
                where {ColumnNames.is_ww_sample}
                {user_where_clause}
                '''
            )
        )

    out_data = list()
    for r in res:
        info = LineageAbundanceWithSampleInfo(
            accession=r[0],
            admin1_name=r[1],
            ww_collected_by=r[2],
            ww_site_id=r[3],
            lineage_name=r[4],
            abundance=r[5],
            ww_viral_load=r[6],
            ww_catchment_population=r[7],
            collection_start_date=r[8],
        )
        out_data.append(info)
    return out_data


async def get_averaged_lineage_abundances_by_location(
    samples_where: str,
    geo_bin: str,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
    lineage_name: str | None = None,
    lineage_system_name: str | None = None
) -> List[AverageLineageAbundanceInfo]:
    # Samples where
    samples_where_clause = ''
    if samples_where is not None:
        samples_where_clause = f'and ({parser.parse(samples_where)})'

    # are we dealing with a wildcard query?
    is_wildcard = lineage_name is not None and lineage_name.endswith('*')

    # lineage name
    if is_wildcard:
        wildcard_lineage = lineage_name
        lineage_name = lineage_name.rstrip('*')
    else:
        wildcard_lineage = None

    # lineage where clause
    params: dict = {}
    lineage_where_clause = ''
    if lineage_name is not None:
        lineage_where_clause = f'{ColumnNames.lineage_name} = :{ColumnNames.lineage_name}'
        params[ColumnNames.lineage_name] = lineage_name
    if lineage_system_name is not None:
        lineage_where_clause = ' and '.join(
            [
                lineage_where_clause,
                f'{ColumnNames.lineage_system_name} = :{ColumnNames.lineage_system_name}'
            ]
        )
        params[ColumnNames.lineage_system_name] = lineage_system_name
    if lineage_where_clause != '':
        lineage_where_clause = 'where ' + lineage_where_clause

    # union to select descendants for wildcard queries
    if is_wildcard:
        child_lineages_union_query = \
            f'''
            union
            select ldc.child_id,
                   ls.id as {ColumnNames.lineage_system_id}
            from lineages_deep_children ldc
            inner join lineages l on l.id = ldc.parent_id
            inner join lineage_systems ls on ls.id = lineage_system_id
            {lineage_where_clause}
            '''
    else:
        child_lineages_union_query = ''

    # geo cols: always use census region, optionally use admin1_name as well.
    # this could be made significantly more flexible
    geo_cols = [ColumnNames.census_region]
    match geo_bin:
        case ColumnNames.admin1_name:
            geo_cols += [ColumnNames.admin1_name]

        case ColumnNames.census_region:
            if 'admin1_name' in samples_where_clause:
                # why not?
                raise ValueError('admin1_name cannot be used in the filter when geo_bin is "census_region"')
        case _:
            raise ValueError(f'illegal value for geo_bin: {geo_bin}')

    # Various query parts, these don't depend on wildcard
    select_geo = ', '.join(geo_cols)
    date_extract_clause = get_extract_clause(COLLECTION_DATE, DateBinOpt.week, 0)
    order_by_clause = get_order_by_cause(DateBinOpt.week)
    date_and_location_cols = get_date_column_names(DateBinOpt.week, False) + geo_cols
    group_by_clause = get_group_by_clause(DateBinOpt.week, extra_cols=geo_cols)
    select_date = get_date_column_names(DateBinOpt.week)

    # when calculating lineage prevalence, we group by lineage id
    # EXCEPT when doing a wildcard query, b/c in that case all present lineages are descendants of the wildcard,
    # and we want to throw them all into one big bucket.
    # Similarly, we skip lineage name and system name in the final select for wildcard queries.
    if is_wildcard:
        group_by_for_lineage_prevalence = ', '.join([group_by_clause, ColumnNames.lineage_system_id])
        select_lineage_id = ''
        select_lineage_name_for_results = ''
        join_lineages_for_results = ''
    else:
        lin_prev_group_cols = date_and_location_cols + [ColumnNames.lineage_system_id, ColumnNames.lineage_id]
        group_by_for_lineage_prevalence = f"group by {', '.join(lin_prev_group_cols)}"
        select_lineage_id = ColumnNames.lineage_id + ','
        select_lineage_name_for_results = ColumnNames.lineage_name + ','
        join_lineages_for_results ='inner join lineages l on l.id = LP.lineage_id'

    query = f'''
            with match_samples as (
                select s.id,
                       {select_geo}, 
                       ww_catchment_population,
                       ww_viral_load,
                       {MID_COLLECTION_DATE_CALCULATION}
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                where (s.collection_end_date - s.collection_start_date) <= {max_span_days}
                  and {ColumnNames.is_ww_sample}
                  {samples_where_clause}
            ),
            base as (
                select {date_extract_clause},
                       l.id as lineage_id,
                       {select_geo},
                       sl.abundance * MS.ww_catchment_population as pop_weighted_prevalence,
                       ww_viral_load,
                       ww_catchment_population
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id
                inner join match_samples MS on MS.id = sl.sample_id
            ),
            overall_prevalences as (
                select {select_date},
                       {select_geo},
                       sum(pop_weighted_prevalence) as overall_prevalence,
                       avg(ww_viral_load) as overall_mean_viral_load,
                       avg(ww_catchment_population) as overall_mean_catchment_population
                from base
                {group_by_clause}
            ),
            match_lineages as (
                select l.id as lineage_id,
                       l.{ColumnNames.lineage_system_id}
                from lineages l
                inner join lineage_systems ls on ls.id = l.lineage_system_id
                {lineage_where_clause}
                {child_lineages_union_query}
            ),
            lineage_prevalences as (
                select {select_lineage_id}
                       {ColumnNames.lineage_system_id},
                       {select_date},
                       {select_geo},
                       sum(base.pop_weighted_prevalence) as lineage_prevalence,
                       count(*) as lineage_sample_count
                from match_lineages
                inner join base using (lineage_id)
                {group_by_for_lineage_prevalence}
            )
            select {select_lineage_name_for_results} 
                   {select_date},
                   {select_geo},
                   {ColumnNames.lineage_system_name},
                   round((LP.lineage_prevalence / OP.overall_prevalence)::numeric, 10) as mean_lineage_prevalence,
                   LP.lineage_sample_count,
                   OP.overall_mean_viral_load, 
                   OP.overall_mean_catchment_population
            from lineage_prevalences LP
            inner join overall_prevalences OP using ({', '.join(date_and_location_cols)})
            {join_lineages_for_results}
            inner join lineage_systems ls on ls.id = LP.lineage_system_id
            {order_by_clause}
            '''

    async with get_async_session() as session:
        res = await session.execute(text(query), params if params else {})

    out_data = list()
    for r in res.mappings():
        year = int(r[YEAR])
        week = int(r[CHUNK])
        epiweek = int(f'{year}{week:02}')
        week_start = date.fromisocalendar(year, week, 1)
        week_end = date.fromisocalendar(year, week, 7)

        if is_wildcard:
            out_lineage_name = wildcard_lineage
        else:
            out_lineage_name = r[ColumnNames.lineage_name]

        out_admin1_name = None
        try:
            out_admin1_name = r[ColumnNames.admin1_name]
        except KeyError:
            pass

        info = AverageLineageAbundanceInfo(
            year=r[YEAR],
            chunk=r[CHUNK],
            epiweek=epiweek,
            week_start=week_start,
            week_end=week_end,
            lineage_name=out_lineage_name,
            lineage_system_name=r[ColumnNames.lineage_system_name],
            census_region=r[ColumnNames.census_region],
            geo_admin1_name=out_admin1_name,
            sample_count=r['lineage_sample_count'],
            mean_viral_load=r['overall_mean_viral_load'],
            mean_catchment_size=r['overall_mean_catchment_population'],
            mean_lineage_prevalence=r['mean_lineage_prevalence']
        )
        out_data.append(info)

    return out_data


async def get_latest_sample(where: str | None) -> List[SampleInfo]:
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and ({parser.parse(where)})'

    sql = (
        f'''
        with latest_date as (
            select s.collection_start_date
            from samples s
            left join geo_locations gl on gl.id = s.geo_location_id
            where s.collection_start_date is not null
                and {ColumnNames.is_ww_sample}
                {user_where_clause}
            order by collection_start_date desc
            limit 1
        )
        select s.*,
               gl.country_name as geo_country_name,
               gl.admin1_name as geo_admin1_name,
               gl.admin2_name as geo_admin2_name,
               gl.admin3_name as geo_admin3_name
        from samples s
        left join geo_locations gl on gl.id = s.geo_location_id
        inner join latest_date using (collection_start_date)
                where {ColumnNames.is_ww_sample}
                {user_where_clause};
        '''
    )

    async with get_async_session() as session:
        res = await session.execute(text(sql))
        return [SampleInfo(**row) for row in res.mappings().all()]


async def count_samples_with_lineage_data(by_col: str, where: str | None = None):
    user_where_clause = ''
    if where is not None and where.strip():
        user_where_clause = f' and {parser.parse(where)}'

    ColumnNames.assert_name_exists(by_col)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                SELECT {by_col}, 
                       count(*) AS count1 
                FROM {TableNames.samples} s 
                LEFT OUTER JOIN {TableNames.geo_locations} gl ON gl.id = s.{ColumnNames.geo_location_id} 
                LEFT OUTER JOIN {TableNames.samples_lineages} sl ON sl.{ColumnNames.sample_id} = s.id 
                WHERE sl.{ColumnNames.abundance} IS NOT NULL
                      and {ColumnNames.is_ww_sample}
                      {user_where_clause}
                GROUP BY {by_col}
                ORDER BY count1 desc
                '''
            ),
        )
        return await _package_count_by_column(res)


async def _package_count_by_column(query_result: Result[tuple[Any, int]] | List[tuple]) -> Dict[str, int]:
    return {str(r[0]): r[1] for r in query_result}


async def count_lineages_by_sample_data(where: str | None = None):
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'and {parser.parse(where)}'

    async with get_async_session() as session:
        res = await session.execute(
            text(
            f'''
            SELECT l.{ColumnNames.lineage_name}, 
                   count(*) AS count1
            FROM {TableNames.samples} s
            LEFT OUTER JOIN {TableNames.geo_locations} gl ON gl.id = s.{ColumnNames.geo_location_id} 
            LEFT OUTER JOIN {TableNames.samples_lineages} sl ON sl.{ColumnNames.sample_id} = s.id 
            LEFT OUTER JOIN {TableNames.lineages} l ON l.id = sl.{ColumnNames.lineage_id}
            WHERE sl.{ColumnNames.abundance} IS NOT NULL
                  and {ColumnNames.is_ww_sample}
                  {user_where_clause} 
            GROUP BY l.{ColumnNames.lineage_name} 
            ORDER BY count1 desc;
            '''
            )
        )
        return await _package_count_by_column(res)
