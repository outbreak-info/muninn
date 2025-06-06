import polars as pl

from DB.engine import get_uri_for_polars
from utils.constants import StandardColumnNames


async def get_all_alleles_as_pl_df() -> pl.DataFrame:
        return pl.read_database_uri(
            query='select * from alleles;',
            uri=get_uri_for_polars()
        ).rename({'id': StandardColumnNames.allele_id})
