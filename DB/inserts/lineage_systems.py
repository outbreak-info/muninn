from sqlalchemy import select

from DB.engine import get_async_write_session
from DB.models import LineageSystem


async def find_or_insert_lineage_system(lineage_system: LineageSystem) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(LineageSystem.id)
            .where(LineageSystem.lineage_system_name == lineage_system.lineage_system_name)
        )
        if id_ is None:
            session.add(lineage_system)
            await session.commit()
            await session.refresh(lineage_system)
            id_ = lineage_system.id
    return id_