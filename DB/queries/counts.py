import datetime
from typing import Type, List, Any, Dict

from sqlalchemy import text, select, Result
from sqlalchemy.sql.functions import func

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation, IntraHostVariant, AminoAcidSubstitution, Allele, Mutation, Translation
from parser.parser import parser
from utils.constants import DateBinOpt
from utils.dates_and_times import format_iso_interval


async def count_samples_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(Sample, GeoLocation)
            .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
            .select_from(Sample)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def count_variants_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(IntraHostVariant, Allele, Translation, AminoAcidSubstitution)
            .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
            .join(Translation, Allele.id == Translation.allele_id, isouter=True)
            .join(
                AminoAcidSubstitution,
                Translation.amino_acid_substitution_id == AminoAcidSubstitution.id,
                isouter=True
            )
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def count_mutations_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(Mutation, Allele, Translation, AminoAcidSubstitution)
            .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
            .join(Translation, Allele.id == Translation.allele_id, isouter=True)
            .join(
                AminoAcidSubstitution,
                Translation.amino_acid_substitution_id == AminoAcidSubstitution.id,
                isouter=True
            )
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def _package_count_by_column(query_result: Result[tuple[Any, int]] | List[tuple]) -> Dict[str, int]:
    return {str(r[0]): r[1] for r in query_result}


async def count_samples_by_simple_date_bin(
    by_col: str,
    date_bin: DateBinOpt,
    days: int | None,
    raw_query: str | None
):
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            result = await _count_samples_by_simple_date_via_extract(by_col, date_bin, where_clause)
        case DateBinOpt.day:
            result = await _count_samples_by_simple_date_custom_days(by_col, days, where_clause)
        case _:
            raise ValueError(f'Illegal value for date_bin: {date_bin}')
    return await _package_count_by_column(result)


# todo: need to add geo_locations to this and a bunch of other queries
async def _count_samples_by_simple_date_via_extract(
    by_col: str,
    date_bin: DateBinOpt,
    where_clause: str
) -> List[tuple]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                   select 
                   extract(year from {by_col}) as year,  
                   extract({date_bin} from {by_col}) as chunk, 
                   count(*)
                   from samples s
                   {where_clause}
                   group by year, chunk 
                   order by year, chunk
                   '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append((date, r[2]))
    return out_data


async def _count_samples_by_simple_date_custom_days(by_col: str, days: int, where_clause: str) -> List[tuple]:
    origin = datetime.date.today()
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select bin_start, bin_start + interval '{days} days' as bin_end, count1 from (
                    select date_bin('{days} days', {by_col}, '{origin}') as bin_start, count(*) as count1
                    from samples s
                    {where_clause}
                    group by bin_start
                    order by bin_start
                )
                '''
            )
        )
        out_data = []
        for r in res:
            interval = f'{r[0]}/{r[1]}'
            count = r[2]
            out_data.append((interval, count))
        return out_data




async def count_samples_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    max_span_days: int,
) -> Dict[str, int]:
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

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
            raise NotImplementedError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                    {extract_clause},
                    count(*)
                from (
                    select 
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select
                        *,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        {where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
               {group_and_order_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        match date_bin:
            case DateBinOpt.month | DateBinOpt.week:
                date = date_bin.format_iso_chunk(r[0], r[1])
            case DateBinOpt.day:
                date = format_iso_interval(r[0], r[1])
            case _:
                raise NotImplementedError
        out_data[date] = r[2]
    return out_data


async def count_variants_by_simple_date_bin(
    date_col: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: str
):
    return await _count_variants_or_mutations_by_simple_date_bin(
        date_col,
        date_bin,
        days,
        raw_query,
        change_bin,
        IntraHostVariant
    )


async def count_mutations_by_simple_date_bin(
    date_col: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: str
):
    return await _count_variants_or_mutations_by_simple_date_bin(
        date_col,
        date_bin,
        days,
        raw_query,
        change_bin,
        Mutation
    )


async def _count_variants_or_mutations_by_simple_date_bin(
    date_col: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: str,
    table: Type['IntraHostVariant'] | Type['Mutation']
):
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            return await _count_v_m_by_simple_date_via_extract(
                table.__tablename__,
                date_col,
                date_bin,
                where_clause,
                change_bin
            )
        case DateBinOpt.day:
            return await _count_v_m_by_simple_date_custom_days(
                table.__tablename__,
                date_col,
                days,
                where_clause,
                change_bin
            )
        case _:
            raise ValueError(f'Illegal value for date_bin: {date_bin}')


async def _count_v_m_by_simple_date_via_extract(
    tablename: str,
    date_col: str,
    date_bin: DateBinOpt,
    where_clause: str,
    change_bin: str
):
    # todo: very smelly
    change_fields = f'ref_{change_bin}, position_{change_bin}, alt_{change_bin}'

    async with get_async_session() as session:
        # todo: this query is really slow for large result sets.
        res = await session.execute(
            text(
                f'''
                select 
                extract(year from {date_col}) as year,
                extract({date_bin} from {date_col}) as chunk,
                count(*),
                region,
                {change_fields}
                from (
                    select region, {change_fields}, {date_col} 
                    from samples s
                    inner join {tablename} VM on VM.sample_id = s.id
                    inner join alleles a on a.id = VM.allele_id
                    left join translations t on t.allele_id = a.id
                    left join amino_acid_substitutions aas on aas.id = t.amino_acid_substitution_id
                    {where_clause}
                )
                group by region, {change_fields}, year, chunk
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[2]
        region = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{region}:{ref}{pos}{alt}'

        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


async def _count_v_m_by_simple_date_custom_days(
    tablename: str,
    date_col: str,
    days: int,
    where_clause: str,
    change_bin: str
):
    # todo: very smelly
    change_fields = f'ref_{change_bin}, position_{change_bin}, alt_{change_bin}'
    origin = datetime.date.today()
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
            select 
            bin_start, 
            bin_start + interval '{days} days' as bin_end, 
            count1,
            region, {change_fields}
            from (
                select 
                date_bin('{days} days', {date_col}, '{origin}') as bin_start, 
                count(*) as count1,
                region, {change_fields}
                from samples s
                inner join {tablename} VM on VM.sample_id = s.id
                inner join alleles a on a.id = VM.allele_id
                left join translations t on t.allele_id = a.id
                left join amino_acid_substitutions aas on aas.id = t.amino_acid_substitution_id
                {where_clause}
                group by bin_start, region, {change_fields}
                order by bin_start
            )
            '''
            )
        )
    out_data = dict()

    for r in res:
        interval = f'{r[0]}/{r[1]}'
        count = r[2]
        region = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{region}:{ref}{pos}{alt}'

        try:
            out_data[interval][change_name] = count
        except KeyError:
            out_data[interval] = {change_name: count}
    return out_data


async def count_lineages_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            return await _count_lineages_by_simple_date_via_extract(group_by, date_bin, where_clause)
        case DateBinOpt.day:
            # todo: implement lineages by custom days
            raise NotImplementedError
        case _:
            raise ValueError(f'Illegal value for date_bin: {date_bin}')


async def _count_lineages_by_simple_date_via_extract(
    group_by: str,
    date_bin: DateBinOpt,
    where_clause: str,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                extract(year from date1) as year,
                extract({date_bin} from date1) as chunk,
                lineage_name,
                lineage_system_name,
                count(*)
                from (
                        select
                        {group_by} as date1,
                        lineage_name,
                        lineage_system_name
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join samples s on s.id = sl.sample_id
                        {where_clause}
                )
                group by year, chunk, lineage_name, lineage_system_name
                order by year, chunk
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
