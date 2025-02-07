from sqlalchemy import create_engine
import os

from utils.constants import Env


def get_url():
    db_user = os.environ[Env.FLU_DB_USER]
    db_password = os.environ[Env.FLU_DB_PASSWORD]
    db_host = os.environ[Env.FLU_DB_HOST]
    db_port = os.environ[Env.FLU_DB_PORT]
    db_name = os.environ[Env.FLU_DB_DB_NAME]
    return f'f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"'

# todo: this way of creating and retrieving the engine is really smelly
def create_pg_engine():
    return create_engine(get_url())

engine = create_pg_engine()