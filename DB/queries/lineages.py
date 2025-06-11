import datetime
from typing import List, Dict

from sqlalchemy import select, func, distinct, text, and_
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import LineageSystem, Lineage, Sample, SampleLineage, GeoLocation
from api.models import LineageCountInfo, LineageAbundanceInfo, LineageInfo, LineageAbundanceSummaryInfo
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa


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


async def get_abundance_summaries_by_simple_date(
    group_by: str,
    raw_query: str,
    date_bin: DateBinOpt,
    days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
               extract(year from {group_by}) as year,
               extract({date_bin} from {group_by}) as chunk
               '''
            group_by_date_cols = 'year, chunk'

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
               date_bin('{days} days', {group_by}, '{origin}') + interval '{days} days' as bin_end,
               date_bin('{days} days', {group_by}, '{origin}') as bin_start
               '''
            group_by_date_cols = 'bin_start'

        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                {extract_clause},
                l.lineage_name,
                ls.lineage_system_name,
                count(*) as samp_count,
                min(sl.abundance) as min,
                percentile_cont(0.25) within group (order by sl.abundance) as q1,
                percentile_cont(0.5) within group (order by sl.abundance) as median,
                percentile_cont(0.75) within group (order by sl.abundance) as q3,
                max(sl.abundance) as max
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id 
                inner join lineage_systems ls on ls.id = l.lineage_system_id 
                inner join samples s on s.id = sl.sample_id 
                left join geo_locations gl on gl.id = s.geo_location_id 
                where sl.is_consensus_call = false {user_where_clause} 
                group by {group_by_date_cols}, l.lineage_name, ls.lineage_system_name
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


async def get_abundance_summaries_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    max_span_days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

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

        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')

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

async def get_mutation_incidence(lineage: str, 
    change_bin: NtOrAa,
    raw_query: str | None
    ):

    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'


    async with get_async_session() as session:

        sampleCount = await session.scalar(
            text(
                f'''
                SELECT count(*)
                FROM lineages
                LEFT JOIN samples_lineages ON samples_lineages.lineage_id = lineages.id
                WHERE lineage_name = :input_lineage
                '''
            ), {
                'input_lineage': lineage
            }
        )

        if change_bin == NtOrAa.nt:
            res = await session.execute(
                text(
                    f'''
                    SELECT region,ref_nt,position_nt,alt_nt,count(*)
                    FROM lineages
                    LEFT JOIN samples_lineages ON samples_lineages.lineage_id = lineages.id
                    LEFT JOIN samples ON samples_lineages.sample_id = samples.id
                    LEFT JOIN mutations ON mutations.sample_id = samples.id
                    LEFT JOIN alleles ON mutations.allele_id = alleles.id
                    WHERE lineage_name = :input_lineage {user_where_clause}
                    GROUP BY region,ref_nt,position_nt,alt_nt
                    ORDER BY count DESC
                    '''
                ), {
                    'input_lineage': lineage
                }
            )
        else:
            res = await session.execute(
                text(
                    f'''
                    SELECT region,
                        ref_aa,
                        position_aa,
                        alt_aa,
                        count(*)
                    FROM lineages
                    LEFT JOIN samples_lineages ON samples_lineages.lineage_id = lineages.id
                    LEFT JOIN mutations ON mutations.sample_id = samples_lineages.sample_id
                    LEFT JOIN alleles ON mutations.allele_id = alleles.id
                    LEFT JOIN translations on translations.allele_id = mutations.allele_id
                    INNER JOIN amino_acid_substitutions ON amino_acid_substitutions.id = amino_acid_substitution_id
                    WHERE lineage_name = :input_lineage
                    GROUP BY region,ref_aa,position_aa,alt_aa
                    ORDER BY count DESC
                    '''
                ), {
                    'input_lineage': lineage
                }
            )

    out = dict()
    for region,ref_nt,position_nt,alt_nt,count in res:
        out[f'{region}:{ref_nt}{position_nt}{alt_nt}'] = count
    return {'samples':sampleCount,'counts':out}