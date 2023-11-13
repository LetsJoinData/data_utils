
SNOWFLAKE_LOAD_FILE_TO_STAGE = "put file://{file_path} @{stage}"

SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE = "copy into {table} from @{stage}/{stage_file_path}"

CREATE_SCHEMA = "create schema if not exists {schema};"

SNOWFLAKE_COPY_STAGE_FILE_TO_TABLE_WITH_COLUMNS_SPECIFIED = """
copy into {schema}.{table}({insert_columns}) 

FROM (
SELECT
{value_columns}
FROM @{stage}/{stage_file_path}
)"""