import polars as pl
import dask.dataframe as dd
import pandas as pd
from sqlalchemy.sql.expression import select

from DB.engine import get_uri_for_polars, get_uri_for_dask
from DB.models import Allele
from utils.constants import StandardColumnNames


async def get_all_alleles_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query='select * from alleles;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.allele_id})



async def get_all_alleles_as_dask_df() -> dd.DataFrame:
    # noinspection PyTypeChecker
    alleles: dd.DataFrame = dd.read_sql_query(
        select(
            Allele.id.label(StandardColumnNames.allele_id),
            Allele.region,
            Allele.position_nt,
            Allele.alt_nt,
            Allele.ref_nt
        ),
        con=get_uri_for_dask(),
        index_col=StandardColumnNames.allele_id,
        # meta=types_frame
    )
    alleles = alleles.astype({StandardColumnNames.position_nt: 'Int32'})
    return alleles
