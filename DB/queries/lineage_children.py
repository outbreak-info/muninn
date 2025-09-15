import polars as pl

from DB.engine import get_uri_for_polars
from utils.constants import StandardColumnNames, TableNames


async def get_all_lineages_immediate_children_by_system_as_pl_df(lineage_system_name: str) -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'''
        select 
            {StandardColumnNames.parent_id},
            {StandardColumnNames.child_id}
        from {TableNames.lineages_immediate_children} lid
        -- we only need to check the parent, they have to be on the same system
        inner join {TableNames.lineages} l on l.id = lid.{StandardColumnNames.parent_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{StandardColumnNames.lineage_system_id}
        where ls.{StandardColumnNames.lineage_system_name} = '{lineage_system_name}'
        ''',
        uri=get_uri_for_polars()
    )
