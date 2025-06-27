from DB.engine import get_async_write_session
from DB.models import SampleLineage
from sqlalchemy import select, and_


async def insert_sample_lineage(sl: SampleLineage) -> int:
    async with get_async_write_session() as session:
        session.add(sl)
        await session.commit()
        await session.refresh(sl)
        id_ = sl.id
    return id_

async def upsert_sample_lineage(sl: SampleLineage) -> bool:
    """
    :param sl: sample lineage to update
    :return: bool: whether an existing record was updated
    """
    modified = False
    async with get_async_write_session() as session:
        existing = await session.scalar(
            select(SampleLineage)
            .where(and_(
                SampleLineage.sample_id == sl.sample_id,
                SampleLineage.lineage_id == sl.lineage_id,
                SampleLineage.is_consensus_call == sl.is_consensus_call
            ))
        )
        if existing is None:
            session.add(sl)
            await session.commit()
        elif (not existing.is_consensus_call) and (existing.abundance != sl.abundance):
            existing.abundance = sl.abundance
            await session.commit()
            modified = True
        # if the existing is a consensus call, then we know that the new one is too
        # and there's no real update to do, since abundance must be null.
    return modified