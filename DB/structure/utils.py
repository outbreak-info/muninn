from pathlib import Path
from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session


async def run_sql_file(filename: str):
    path = Path(__file__).parent / filename
    with open(path, 'r') as f:
        query = ''.join(f.readlines())
    async with get_async_write_session() as session:
        await session.execute(text(query))
        await session.commit()
