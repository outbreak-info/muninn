import polars as pl
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError

from DB.engine import get_async_session
from DB.models import Translation


async def insert_translation(t: Translation):
    async with get_async_session() as session:
        # todo: is there a cleaner way to get this behavior?
        try:
            session.add(t)
            await session.commit()
        except IntegrityError:
            pass


async def batch_insert_translations(translations: pl.DataFrame) -> None:
    async with get_async_session() as session:
        await session.execute(
            insert(Translation),
            translations.to_dicts()
        )
        await session.commit()
