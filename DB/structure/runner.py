from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from DB.structure import sequences, samples, geo_locations, alleles, amino_acids


async def set_up_db():
    await sequences.create_all()
    await geo_locations.create_all()
    await samples.create_all()
    await alleles.create_all()
    await amino_acids.create_all()


async def run_sql_file(filename: str):
    with open(filename, 'r') as f:
        query = ''.join(f.readlines())
    async with get_async_write_session() as session:
        await session.execute(text(query))
        await session.commit()