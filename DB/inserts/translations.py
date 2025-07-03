import polars as pl
from sqlalchemy.exc import IntegrityError

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Translation
from utils.constants import StandardColumnNames


async def insert_translation(t: Translation):
    async with get_async_write_session() as session:
        # todo: is there a cleaner way to get this behavior?
        try:
            session.add(t)
            await session.commit()
        except IntegrityError:
            pass


async def copy_insert_translations(translations: pl.DataFrame) -> str:
    columns = [
        StandardColumnNames.allele_id,
        StandardColumnNames.amino_acid_id
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Translation.__tablename__,
        records=translations.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res
