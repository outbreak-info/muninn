from DB.engine import get_uri_for_polars
import polars as pl

from utils.constants import StandardColumnNames


async def get_all_translations_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query='select * from translations;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.translation_id})
