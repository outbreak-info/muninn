from typing import List, Type

from sqlalchemy import select, func, distinct, text
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import LineageSystem, Lineage, IntraHostVariant, Sample, SampleLineage, GeoLocation, Mutation
from api.models import LineageCountInfo
from parser.parser import parser


def get_sample_counts_by_lineage_via_variant(query: str) -> List[LineageCountInfo]:
    return _get_sample_counts_by_lineage_via_table(IntraHostVariant, query)


def get_sample_counts_by_lineage_via_mutation(query: str) -> List[LineageCountInfo]:
    return _get_sample_counts_by_lineage_via_table(Mutation, query)


def _get_sample_counts_by_lineage_via_table(
    via_table: Type[Mutation] | Type[IntraHostVariant],
    query: str
) -> List[LineageCountInfo]:
    user_query = parser.parse(query)
    with Session(engine) as session:
        res = session.execute(
            select(via_table, SampleLineage, Lineage, LineageSystem)
            .join(SampleLineage, SampleLineage.sample_id == via_table.sample_id, isouter=True)
            .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
            .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
            .where(
                via_table.sample_id.in_(
                    select(Sample.id)
                    .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                    .where(text(user_query))
                )
            )
            .with_only_columns(
                LineageSystem.lineage_system_name,
                Lineage.lineage_name,
                func.count(distinct(via_table.sample_id)).label('count1')
            )
            .group_by(LineageSystem.lineage_system_name, Lineage.lineage_name)
            .order_by(text('count1 desc'))
        ).all()

        # out_data = [LineageCountInfo(count=r[2], lineage_system=r[0], lineage=r[1]) for r in res]
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
