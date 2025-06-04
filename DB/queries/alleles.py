import polars as pl

from DB.engine import async_engine
from utils.constants import StandardColumnNames


async def get_all_alleles_as_pl_df() -> pl.DataFrame:
        return pl.read_database_uri(
            query='select * from alleles;',
            uri=async_engine.url.render_as_string(hide_password=False).replace('+asyncpg', '') # todo
        ).rename({'id': StandardColumnNames.allele_id})
