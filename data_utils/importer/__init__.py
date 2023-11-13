from data_utils.sql_helpers.query_helpers import copy_snowflake_stage_file_to_table, add_file_to_snowflake_stage
from data_utils.services.sftp import SFTP
import os
import logging
import pandas as pd

TEMP_FOLDER = os.environ.get('TMP_FOLDER_PATH','.tmp')


class SNOWFLAKE_LOADER:
    FILEPATH_COLUMN_ALIAS = 'DW_FILENAME'
    CREATED_AT_COLUMN_ALIAS = 'DW_CREATED_AT'
    METADATA_COLUMNS_SCHEMA = {
        "dw_filename": "varchar(2056)",
        "dw_created_at": "timestamp",
        "dw_updated_at": "timestamp default current_timestamp()"
    }


    def __init__(self, cursor):
        self.cursor = cursor


    def generate_metadata_table_definition(self):
        meta_def = ",\n".join( f"{column}  {column_definition}" for column, column_definition in self.METADATA_COLUMNS_SCHEMA.items())
        return meta_def

    def _get_columns_to_load(self, local_file_path, delimiter='|'):
        metadata_columns = ['DW_FILENAME','DW_CREATED_AT']
        df = pd.read_csv(local_file_path, delimiter=delimiter, nrows=1)
        data_columns = df.columns.to_list()
        insert_columns = ",".join(data_columns + metadata_columns)

        value_columns_alias = ",".join([f"${i} as {col}" for i,col in enumerate(data_columns,1)])
        metadata_values = f"metadata$filename as {metadata_columns[0]}, current_timestamp() as {metadata_columns[1]}"
        value_columns = f"{value_columns_alias},{metadata_values}"

        return insert_columns, value_columns
        

    def snowflake_stage_and_load_csv_to_table(self, db_table, db_stage, file_path, db_schema=None, file_delimiter='|', delete_local_file=True):
        file_name = os.path.basename(file_path)
        insert_columns, value_columns = self._get_columns_to_load(file_path, file_delimiter)
        if delete_local_file:
            if os.path.exists(file_path):
                os.remove(file_path)

        # Loading Stage file to table
        add_file_to_snowflake_stage(self.cursor,file_path,db_stage)
        copy_snowflake_stage_file_to_table(cursor=self.cursor, 
                                        table=db_table, 
                                        stage=db_stage,
                                        stage_file_path=file_path,
                                        schema=db_schema,
                                        insert_columns=insert_columns,
                                        value_columns=value_columns)
            
        logging.info(f"Successfully Loaded {file_name} to table")

    
    def snowflake_stage_and_load_df_to_table(self, db_table, db_stage, df:pd.DataFrame, file_name, db_schema=None, delete_local_file=True):
        file_delimiter=','
        file_path = f"{TEMP_FOLDER}/{file_name}"
        df.to_csv(file_path, file_delimiter, index=False)

        self.stage_and_load_csv_to_table(db_table, db_stage, file_path, db_schema, file_delimiter, True)
        if delete_local_file:
            os.remove(file_path)


class SFTP_SNOWFLAKE_LOADER(SNOWFLAKE_LOADER):
    def __init__(self, sftp_connection, **sftp_other_sftp_kwargs):

        self.sftp_other_sftp_kwargs = sftp_other_sftp_kwargs
        self.sftp_connection = sftp_connection

        super.__init__(schema, table, stage)


    def _get_columns_to_load(self, local_file_path, delimiter='|'):
        metadata_columns = ['DW_FILENAME','DW_CREATED_AT']
        df = pd.read_csv(local_file_path, delimiter=delimiter, nrows=1)
        data_columns = df.columns.to_list()
        insert_columns = ",".join(data_columns + metadata_columns)

        value_columns_alias = ",".join([f"${i} as {col}" for i,col in enumerate(data_columns,1)])
        metadata_values = f"metadata$filename as {metadata_columns[0]}, current_timestamp() as {metadata_columns[1]}"
        value_columns = f"{value_columns_alias},{metadata_values}"

        return insert_columns, value_columns
        

    def _download_sftp_file(self, sftp_file_name):
        local_file_path = self.sftp_connection.download_file(sftp_file_name)

        return local_file_path
    

    def _add_file_to_stage(self, local_file_path, db_stage):
        file_name = os.path.basename(local_file_path)
        stage_file_path = add_file_to_snowflake_stage(self.cur, local_file_path, db_stage)

        return stage_file_path
    

    def _load_stage_file_to_table(self, db_table, db_stage, stage_file_path, local_file_path, db_schema=None, file_delimiter='|', delete_local_file=True):
        file_name = os.path.basename(local_file_path)
        insert_columns, value_columns = self._get_columns_to_load(local_file_path, file_delimiter)
        if delete_local_file:
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

        # Loading Stage file to table
        copy_snowflake_stage_file_to_table(cursor=self.cursor, 
                                        table=db_table, 
                                        stage=db_stage,
                                        stage_file_path=stage_file_path,
                                        schema=db_schema,
                                        insert_columns=insert_columns,
                                        value_columns=value_columns)
            
        logging.info(f"Successfully Loaded {file_name} to table")


    def load_sftp_file_to_table(self, db_table, db_stage, sftp_file_name, db_schema=None, file_delimiter='|', delete_local_file=True):

        local_file_path = self._download_sftp_file(sftp_file_name)
        stage_file_path = self._add_file_to_stage(local_file_path, db_stage)
        self._load_stage_file_to_table(db_table, db_stage, stage_file_path, local_file_path, db_schema, file_delimiter, delete_local_file)


