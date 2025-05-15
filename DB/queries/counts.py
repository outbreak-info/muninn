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


async def count_variants_or_mutations_by_simple_date(
    by_col: str,
    var_table: Type[IntraHostVariant] | Type[Mutation],
    interval: str
) -> List[tuple]:
    """
    This will be for release_date and creation_date
    :param by_col:
    :param var_table:
    :param interval:
    :return:
    """

    if by_col not in {'creation_date', 'release_date'}:
        raise ValueError  # todo

    tablename = var_table.__tablename__

    match interval:
        case 'isoweek':
            return await _count_v_or_m_by_simple_date_iso_week(by_col, tablename)
        case _:
            days = int(interval)
            return await _count_v_or_m_by_simple_date_custom_days(by_col, tablename, days)


async def _count_v_or_m_by_simple_date_iso_week(
    by_col: str,
    tablename: str,
) -> List[tuple]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                extract(year from {by_col}) as year,  
                extract(week from {by_col}) as week, 
                count(*)
                from samples s
                left join {tablename} VM on VM.sample_id = s.id
                group by year, week 
                '''
            )
        )
    out_data = []
    for r in res:
        # todo: some real datetime stuff for this
        week_stamp = f'{r[0]}-W{r[1]}'
        out_data.append((week_stamp, r[2]))
    return out_data


async def _count_v_or_m_by_simple_date_custom_days(by_col: str, tablename: str, days: int) -> List[tuple]:
    # todo: for now I'm going to ignore complications
    origin = datetime.date.today()

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select bin_start, bin_start + interval '{days} days' as bin_end, count1 from (
                    select bin_start, count(distinct vm_id) as count1 from (
                        select date_bin('{days} days', {by_col}, '{origin}') as bin_start, vm.id as vm_id 
                        from samples s
                        left join {tablename} vm on vm.sample_id = s.id
                    )
                    group by bin_start
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
