from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/geo_locations/create_geo_locations_table.sql')
    await run_sql_file('sql/geo_locations/create_pk_geo_locations.sql')
    await run_sql_file('sql/geo_locations/create_uq_division_names.sql')