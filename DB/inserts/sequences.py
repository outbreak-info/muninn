from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from utils.constants import TableNames


async def insert_sequences_for_row_numbers(row_numbers: list[int]) -> dict[int, int]:
    async with get_async_write_session() as session:
        r = await session.execute(text(
            f'insert into {TableNames.sequences} '
            f'select from generate_series(1, {len(row_numbers)}) returning id;'
        ))
        await session.commit()
    return dict(zip(row_numbers, r.scalars().all()))