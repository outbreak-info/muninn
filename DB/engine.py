import os

import asyncpg
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from utils.constants import Env

STATEMENT_TIMEOUT_MS = 600_000
POOL_SIZE = 10
MAX_OVERFLOW = (90 - (2 * POOL_SIZE)) / 2


async def get_asyncpg_connection():
    db_user = os.environ[Env.FLU_DB_SUPERUSER]
    db_password = os.environ[Env.FLU_DB_SUPERUSER_PASSWORD]
    db_host = os.environ[Env.FLU_DB_HOST]
    db_port = int(os.environ[Env.FLU_DB_PORT])
    db_name = os.environ[Env.FLU_DB_DB_NAME]

    return await asyncpg.connect(
        host=db_host,
        user=db_user,
        port=db_port,
        password=db_password,
        database=db_name
    )


def get_url(async_: bool = False, polars: bool = False, readonly: bool = True):
    db_user = os.environ[Env.FLU_DB_READONLY_USER] if readonly else os.environ[Env.FLU_DB_SUPERUSER]
    db_password = os.environ[Env.FLU_DB_READONLY_PASSWORD] if readonly else os.environ[Env.FLU_DB_SUPERUSER_PASSWORD]
    db_host = os.environ[Env.FLU_DB_HOST]
    db_port = int(os.environ[Env.FLU_DB_PORT])
    db_name = os.environ[Env.FLU_DB_DB_NAME]

    drivername = 'postgresql+psycopg2'
    if polars:
        drivername = 'postgresql'
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


def create_pg_engine():
    return create_engine(
        get_url(readonly=False),
        connect_args={'options': f'-c statement_timeout={STATEMENT_TIMEOUT_MS}'}
    )


async_write_engine = create_async_engine(
    get_url(async_=True, readonly=False),
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    connect_args={'server_settings': {'statement_timeout': str(STATEMENT_TIMEOUT_MS)}}
)

async_engine = create_async_engine(
    get_url(async_=True),
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    connect_args={'server_settings': {'statement_timeout': str(STATEMENT_TIMEOUT_MS)}}
)


def get_async_write_session():
    return AsyncSession(async_write_engine, expire_on_commit=False)


def get_async_session():
    return AsyncSession(async_engine, expire_on_commit=False)


def get_uri_for_polars():
    return get_url(polars=True).render_as_string(hide_password=False)
