import datetime
from enum import Enum
from typing import Type, List

from sqlalchemy import text, select
from sqlalchemy.sql.functions import func

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation, IntraHostVariant, AminoAcidSubstitution, Allele, Mutation, Translation


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
        return res


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
        return res


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
        return res


async def count_variants_mutations_or_samples_by_simple_date(
    by_col: str,
    count_table: Type[IntraHostVariant] | Type[Mutation] | Type[Sample],
    interval: str
) -> List[tuple]:
    """
    This will be for release_date and creation_date
    :param by_col:
    :param count_table:
    :param interval:
    :return:
    """
    # todo: All of these need some attention paid to timezone
    #  If release date is 2025-05-31 23:00:00 PDT, it's already June in UTC. Which month should that count under?
    #  I think local time should be respected. I'm not sure how these dates are being stored right now.
    if by_col not in {'creation_date', 'release_date'}:
        raise ValueError  # todo

    join_clause = f'inner join {count_table.__tablename__} VM on VM.sample_id = s.id'
    if count_table is Sample:
        join_clause = ''

    match interval:
        case 'isoweek':
            return await _count_v_m_s_by_simple_date_iso_week(by_col, join_clause)
        case 'month':
            return await _count_v_m_s_by_simple_date_month(by_col, join_clause)
        case _:
            days = int(interval)
            return await _count_v_m_s_by_simple_date_custom_days(by_col, join_clause, days)


async def _count_v_m_s_by_simple_date_iso_week(by_col: str, join_clause: str) -> List[tuple]:
    res = await _count_v_m_s_by_simple_date_via_extract(by_col, join_clause, 'week')
    out_data = []
    for r in res:
        # todo: some real datetime stuff for this
        week_stamp = f'{r[0]}-W{r[1]}'
        out_data.append((week_stamp, r[2]))
    return out_data


async def _count_v_m_s_by_simple_date_month(by_col: str, join_clause: str) -> List[tuple]:
    res = await _count_v_m_s_by_simple_date_via_extract(by_col, join_clause, 'month')
    out_data = []
    for r in res:
        # todo: need a good way to deal with iso8601 months (2025-05) b/c datetime won't do it.
        padded_month = str(r[1]).rjust(2, '0')
        month_stamp = f'{r[0]}-{padded_month}'
        out_data.append((month_stamp, r[2]))
    return out_data


async def _count_v_m_s_by_simple_date_via_extract(by_col: str, join_clause: str, interval: str) -> List[tuple]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                   select 
                   extract(year from {by_col}) as year,  
                   extract({interval} from {by_col}) as chunk, 
                   count(*)
                   from samples s
                   {join_clause}
                   group by year, chunk 
                   order by year, chunk
                   '''
            )
        )
    return res


async def _count_v_m_s_by_simple_date_custom_days(by_col: str, join_clause: str, days: int) -> List[tuple]:
    origin = datetime.date.today()
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select bin_start, bin_start + interval '{days} days' as bin_end, count1 from (
                    select date_bin('{days} days', {by_col}, '{origin}') as bin_start, count(*) as count1
                    from samples s
                    {join_clause}
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
