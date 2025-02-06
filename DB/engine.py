from sqlalchemy import create_engine
import os

# todo: this way of creating and retrieving the engine is really smelly
def create_pg_engine():
    db_name = "flu"
    db_user = "flu"
    db_password = os.environ['FLU_DB_PASSWORD']
    db_host = "localhost"
    db_port = 5432
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

engine = create_pg_engine()