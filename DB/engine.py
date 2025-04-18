import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from utils.constants import Env


def get_url(async_: bool = False):
    db_user = os.environ[Env.FLU_DB_USER]
    db_password = os.environ[Env.FLU_DB_PASSWORD]
    db_host = os.environ[Env.FLU_DB_HOST]
    db_port = int(os.environ[Env.FLU_DB_PORT])
    db_name = os.environ[Env.FLU_DB_DB_NAME]

    drivername = 'postgresql+psycopg2'
    if async_:
        drivername = 'postgresql+asyncpg'

    url = URL.create(
        drivername=drivername,
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database=db_name
    )

    return url

# todo: this way of creating and retrieving the engine is really smelly
def create_pg_engine():
    return create_engine(get_url())

engine = create_pg_engine()

async_engine = create_async_engine(get_url(async_=True))

def get_async_session():
    return AsyncSession(async_engine, expire_on_commit=False)
