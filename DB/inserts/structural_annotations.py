from DB.engine import get_async_write_session
from DB.models import StructuralAnnotation


async def insert_structural_annotation(sa_row: StructuralAnnotation) -> int:
    async with get_async_write_session() as session:
        session.add(sa_row)
        await session.commit()
        await session.refresh(sa_row)
        return sa_row.id