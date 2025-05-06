from typing import List

from sqlalchemy import select, func, distinct, text, and_
from sqlalchemy.orm import Session, contains_eager

from DB.engine import engine, get_async_session
from DB.models import LineageSystem, Lineage, Sample, SampleLineage, GeoLocation
from api.models import LineageCountInfo, LineageAbundanceInfo, LineageInfo
from parser.parser import parser


def get_sample_counts_by_lineage(samples_raw_query: str | None) -> List[LineageCountInfo]:
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

    with Session(engine) as session:
        res = session.execute(lineage_count_query)
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