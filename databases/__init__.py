import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import snowflake


def create_snowflake_connection(account, user, password, database, warehouse, autocommit=False):
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse,
        autocommit=autocommit
    )


@contextmanager
def create_snowflake_connection_and_cursor(account, user, password, database, warehouse, autocommit=False):
    with create_snowflake_connection(account, user, password, database, warehouse, autocommit) as conn:
        yield conn.cursor(snowflake.connector.DictCursor)


def create_snowflake_engine(user,password,account,database,warehouse):
    return create_engine(
        URL(
            user=user,
            password=password,
            account=account,
            database=database,
            warehouse=warehouse,
        )
)
