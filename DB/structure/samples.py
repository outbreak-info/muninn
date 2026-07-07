from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/samples/create_table_samples.sql')
    await run_sql_file('sql/samples/create_pk_samples.sql')
    await run_sql_file('sql/samples/create_uq_accession.sql')
    await run_sql_file('sql/samples/create_fk_sequence_id.sql')
    await run_sql_file('sql/samples/create_fk_geo_location_id.sql')
    await run_sql_file('sql/samples/create_check_collection_dates_exist.sql')
    await run_sql_file('sql/samples/create_check_collection_start_before_end.sql')
    await run_sql_file('sql/samples/create_check_retraction_values.sql')
