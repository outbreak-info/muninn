import logging
from typing import Any

import polars as pl
from sqlalchemy import select, and_, text, insert

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Allele

from utils.constants import StandardColumnNames


async def find_or_insert_allele(a: Allele) -> int:
    async with get_async_write_session() as session:
        id_: int = await session.scalar(
            select(Allele.id)
            .where(
                and_(
                    Allele.region == a.region,
                    Allele.position_nt == a.position_nt,
                    Allele.alt_nt == a.alt_nt
                )
            )
        )

        if id_ is None:
            session.add(a)
            await session.commit()
            await session.refresh(a)
            id_ = a.id
    return id_
