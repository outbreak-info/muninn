from sqlalchemy import create_engine


def create_pg_engine():
    db_name = "postgres"
    db_user = "postgres"
    db_password = ""
    db_host = "localhost"
    db_port = 5432
    return create_engine(f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")
