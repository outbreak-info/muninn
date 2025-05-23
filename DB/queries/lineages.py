import datetime
from typing import List, Dict

from sqlalchemy import select, func, distinct, text, and_
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import LineageSystem, Lineage, Sample, SampleLineage, GeoLocation
from api.models import LineageCountInfo, LineageAbundanceInfo, LineageInfo, LineageAbundanceSummaryInfo
from parser.parser import parser
from utils.constants import DateBinOpt


async def get_sample_counts_by_lineage(samples_raw_query: str | None) -> List[LineageCountInfo]:
    lineage_count_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .with_only_columns(
            LineageSystem.lineage_system_name,
            Lineage.lineage_name,
            func.count(distinct(SampleLineage.sample_id)).label('count1')
        )
        .group_by(LineageSystem.lineage_system_name, Lineage.lineage_name)
        .order_by(text('count1 desc'))
    )

    if samples_raw_query is not None:
        user_query = parser.parse(samples_raw_query)
        lineage_count_query = lineage_count_query.where(
            SampleLineage.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )

    async with get_async_session() as session:
        res = await session.execute(lineage_count_query)
        out_data = []
        for r in res:
            out_data.append(
                LineageCountInfo(
                    count=r[2],
                    lineage=r[1],
                    lineage_system=r[0]
                )
            )
    return out_data


async def get_abundances(raw_query: str | None) -> List[LineageAbundanceInfo]:
    lineage_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .options(contains_eager(SampleLineage.r_lineage))
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .options(contains_eager(Lineage.r_lineage_system))
        .join(Sample, Sample.id == SampleLineage.sample_id, isouter=True)
        .options(contains_eager(SampleLineage.r_sample))
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
    )

    if raw_query is not None:
        user_query = parser.parse(raw_query)
        lineage_query = lineage_query.where(
            and_(
                SampleLineage.is_consensus_call == False,
                text(user_query)
            )
        )
    else:
        lineage_query = lineage_query.where(
            SampleLineage.is_consensus_call == False
        )

    out_data = []
    async with get_async_session() as session:
        res: List[SampleLineage] = await session.scalars(lineage_query)
        for samp_lin in res:
            out_data.append(
                LineageAbundanceInfo(
                    lineage_info=LineageInfo(
                        lineage_id=samp_lin.lineage_id,
                        lineage_name=samp_lin.r_lineage.lineage_name,
                        lineage_system_name=samp_lin.r_lineage.r_lineage_system.lineage_system_name,
                        lineage_system_id=samp_lin.r_lineage.lineage_system_id,
                    ),
                    sample_id=samp_lin.sample_id,
                    accession=samp_lin.r_sample.accession,
                    abundance=samp_lin.abundance
                )
            )
    return out_data


async def get_abundance_summaries(raw_query: str | None) -> List[LineageAbundanceSummaryInfo]:
    lineage_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .options(contains_eager(SampleLineage.r_lineage))
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .options(contains_eager(Lineage.r_lineage_system))
        .join(Sample, Sample.id == SampleLineage.sample_id, isouter=True)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .with_only_columns(
            Lineage.lineage_name,
            LineageSystem.lineage_system_name,
            func.count().label('samp_count'),
            func.min(SampleLineage.abundance).label('min'),
            func.percentile_cont(0.25).within_group(SampleLineage.abundance).label('q1'),
            func.percentile_cont(0.5).within_group(SampleLineage.abundance).label('median'),
            func.percentile_cont(0.75).within_group(SampleLineage.abundance).label('q3'),
            func.max(SampleLineage.abundance).label('max'),
        )
        .group_by(Lineage.lineage_name, LineageSystem.lineage_system_name)
    )

    if raw_query is not None:
        user_query = parser.parse(raw_query)
        lineage_query = lineage_query.where(
            and_(
                SampleLineage.is_consensus_call == False,
                text(user_query)
            )
        )
    else:
        lineage_query = lineage_query.where(
            SampleLineage.is_consensus_call == False
        )

    out_data = []
    async with get_async_session() as session:
        res = await session.execute(lineage_query)
        for r in res:
            out_data.append(
                LineageAbundanceSummaryInfo(
                    lineage_name=r[0],
                    lineage_system_name=r[1],
                    sample_count=r[2],
                    abundance_min=r[3],
                    abundance_q1=r[4],
                    abundance_median=r[5],
                    abundance_q3=r[6],
                    abundance_max=r[7]
                )
            )
    return out_data


async def get_abundance_summaries_by_date(
    group_by: str,
    raw_query: str,
    date_bin: DateBinOpt,
    days: int,
):
    where_clause = 'where sl.is_consensus_call = false'
    if raw_query is not None:
        where_clause = f'{where_clause} and {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            return await _get_abundance_summaries_by_date_via_extract(group_by, where_clause, date_bin)
        case DateBinOpt.day:
            return await _get_abundance_summaries_by_date_custom_days(group_by, where_clause, days)
        case _:
            raise ValueError(f'Illegal value for date_bin: {date_bin}')


async def _get_abundance_summaries_by_date_via_extract(
    group_by: str,
    where_clause: str,
    date_bin: DateBinOpt
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
            select 
            l.lineage_name,
            ls.lineage_system_name,
            count(*) as samp_count,
            min(sl.abundance) as min,
            percentile_cont(0.25) within group (order by sl.abundance) as q1,
            percentile_cont(0.5) within group (order by sl.abundance) as median,
            percentile_cont(0.75) within group (order by sl.abundance) as q3,
            max(sl.abundance) as max,
            extract(year from {group_by}) as year,
            extract({date_bin} from {group_by}) as chunk
            from samples_lineages sl
            inner join lineages l on l.id = sl.lineage_id 
            inner join lineage_systems ls on ls.id = l.lineage_system_id 
            inner join samples s on s.id = sl.sample_id 
            left join geo_locations gl on gl.id = s.geo_location_id 
            {where_clause} 
            group by year, chunk, l.lineage_name, ls.lineage_system_name
            '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[8], r[9])
        info = LineageAbundanceSummaryInfo(
            lineage_name=r[0],
            lineage_system_name=r[1],
            sample_count=r[2],
            abundance_min=r[3],
            abundance_q1=r[4],
            abundance_median=r[5],
            abundance_q3=r[6],
            abundance_max=r[7]
        )
        try:
            out_data[date].append(info)
        except KeyError:
            out_data[date] = [info]
    return out_data


async def _get_abundance_summaries_by_date_custom_days(
    group_by: str,
    where_clause: str,
    days: int
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    origin = datetime.date.today()
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                lineage_name,
                lineage_system_name,
                samp_count,
                min,
                q1,
                median,
                q3,
                max,
                bin_start,
                bin_start + interval '{days} days' as bin_end
                from (
                    select 
                    l.lineage_name,
                    ls.lineage_system_name,
                    count(*) as samp_count,
                    min(sl.abundance) as min,
                    percentile_cont(0.25) within group (order by sl.abundance) as q1,
                    percentile_cont(0.5) within group (order by sl.abundance) as median,
                    percentile_cont(0.75) within group (order by sl.abundance) as q3,
                    max(sl.abundance) as max,
                    date_bin('{days} days', {group_by}, '{origin}') as bin_start
                    from samples_lineages sl
                    inner join lineages l on l.id = sl.lineage_id 
                    inner join lineage_systems ls on ls.id = l.lineage_system_id 
                    inner join samples s on s.id = sl.sample_id 
                    left join geo_locations gl on gl.id = s.geo_location_id 
                    {where_clause} 
                    group by bin_start, l.lineage_name, ls.lineage_system_name
                )
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = f'{r[8]}/{r[9]}'
        info = LineageAbundanceSummaryInfo(
            lineage_name=r[0],
            lineage_system_name=r[1],
            sample_count=r[2],
            abundance_min=r[3],
            abundance_q1=r[4],
            abundance_median=r[5],
            abundance_q3=r[6],
            abundance_max=r[7]
        )
        try:
            out_data[date].append(info)
        except KeyError:
            out_data[date] = [info]
    return out_data


async def get_abundance_summaries_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    max_span_days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    extract_clause = ''
    group_by_date_cols = ''

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from mid_collection_date) as year,
            extract({date_bin} from mid_collection_date) as chunk
            '''

            group_by_date_cols = 'year, chunk'

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            group_by_date_cols = 'bin_start'

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
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from(
                        select 
                        l.lineage_name,
                        ls.lineage_system_name,
                        sl.abundance,
                        collection_start_date,
                        collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id 
                        inner join lineage_systems ls on ls.id = l.lineage_system_id 
                        inner join samples s on s.id = sl.sample_id 
                        left join geo_locations gl on gl.id = s.geo_location_id 
                        where sl.is_consensus_call = false {user_where_clause}
                        
                    )
                    where collection_span <= {max_span_days}
                )
                group by {group_by_date_cols}, lineage_name, lineage_system_name
                order by {group_by_date_cols}
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