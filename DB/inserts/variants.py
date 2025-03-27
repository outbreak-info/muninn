from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import IntraHostVariant


async def find_or_insert_variant(variant: IntraHostVariant) -> (int, bool):
    """
    :param variant:
    :return: int: id of variant, new or existing
    bool: true if this variant already existed
    """
    preexisting = True
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(IntraHostVariant.id)
            .where(
                and_(
                    IntraHostVariant.allele_id == variant.allele_id,
                    IntraHostVariant.sample_id == variant.sample_id
                )
            )
        )
        if id_ is None:
            preexisting = False
            session.add(variant)
            await session.commit()
            await session.refresh(variant)
            id_ = variant.id
    return id_, preexisting
