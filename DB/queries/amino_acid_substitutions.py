import polars as pl

from DB.engine import get_uri_for_polars
from utils.constants import StandardColumnNames


async def get_all_amino_acid_subs_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query='select * from amino_acid_substitutions;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.amino_acid_substitution_id})
