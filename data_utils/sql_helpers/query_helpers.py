import logging
from data_utils.sql_helpers.templates import (SNOWFLAKE_LOAD_FILE_TO_STAGE, SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE,CREATE_SCHEMA,
                                             SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE_WITH_COLUMNS_SPECIFIED, CREATE_STAGE, CREATE_TEMPORARY_STAGE)
import os
import logging
from uuid import uuid1


def execute_sql(cursor, sql, sql_vars=None):
    all_sql = sql.split(';')

    for sqlcmd in all_sql:
        # Ignore any trailing whitespace that shows up as the final item
        if sqlcmd.strip():
            sqlcmd = sqlcmd + ';'
            logging.info(f'Executing sql: {sqlcmd}')
            cursor.execute(sqlcmd, sql_vars)


def query_all(cursor, sql, sql_vars=None):
    execute_sql(cursor, sql, sql_vars=sql_vars)
    return cursor.fetchall()


def query_all_by_key(cursor, sql, key):
    """This is designed for resultsets that realistically fit
    in memory. It is NOT DESIGNED FOR ABSURD NUMBERS OF ROWS.
    """

    logging.info(f'Querying all results for key "{key}"...')
    results = query_all(cursor, sql, sql_vars=None)
    return [result[key] for result in results]


def add_file_to_snowflake_stage(cursor, file_path, stage, stage_folder=None) -> str:
    folder = f"/{stage_folder}/" if stage_folder else "" 
    sql = SNOWFLAKE_LOAD_FILE_TO_STAGE.format(file_path=file_path, stage=stage, folder=folder)
    execute_sql(cursor, sql)
    stage_file_path = "{}{}".format(folder, os.path.basename(file_path))
    logging.info(f"Successfully staged file to snowflake. File:{stage_file_path}")
    return stage_file_path


def copy_snowflake_stage_file_to_table(cursor, table, stage, stage_file_path, schema=None, insert_columns=None, value_columns=None):

    if not insert_columns:
        sql = SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE.format(schema=schema, 
                                                        table=table,
                                                        stage=stage,
                                                        stage_file_path=stage_file_path)
    else:
        sql = SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE_WITH_COLUMNS_SPECIFIED.format(schema=schema, 
                                                                                table=table,
                                                                                stage=stage,
                                                                                stage_file_path=stage_file_path,
                                                                                insert_columns=insert_columns,
                                                                                value_columns=value_columns)
    execute_sql(cursor, sql)
    logging.info(f"Successfully loaded stage file to table. File:{stage_file_path}")


def create_new_schema(cursor, schema, database=None):
    schema_name = schema if not database else f"{database}.{schema}"
    sql = CREATE_SCHEMA.format(schema=schema_name)
    execute_sql(cursor, sql)


def create_stage(cursor, schema, stage_name=None, temporary_stage=False, format_params=''):
    temp_stage_name = str(uuid1()).replace('-','')
    stage_name = stage_name or temp_stage_name

    if not stage_name or temporary_stage:
        sql = CREATE_TEMPORARY_STAGE.format(params=format_params, schema=schema, stage_name=stage_name)
    else:
        sql = CREATE_STAGE.format(params=format_params, schema=schema, stage_name=stage_name)
    execute_sql(cursor, sql)

    return f"{schema}.{stage_name}"
