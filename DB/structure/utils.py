from pathlib import Path
from DB.textutils import text

from DB.engine import get_async_write_session
from utils.constants import TableNames


async def run_sql_file(filename: str):
    path = Path(__file__).parent / filename
    with open(path, 'r') as f:
        query = ''.join(f.readlines())
    async with get_async_write_session() as session:
        await session.execute(text(query))
        await session.commit()


async def refresh_matview(name: str):
    TableNames.assert_name_exists(name)
    async with get_async_write_session() as session:
        await session.execute(
            text(
                f'refresh materialized view {name};'
            )
        )
        await session.commit()

