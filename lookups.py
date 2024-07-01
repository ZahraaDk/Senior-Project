from enum import Enum 

class ErrorHandling(Enum):
    ERROR_CONNECTING_TO_DB = "Error connecting to the database"
    DB_RETURN_QUERY_ERROR = "Error returning this query"
    RETURN_DATA_CSV_ERROR = "Error returning the csv files"
    RETURN_DATA_EXCEL_ERROR = "Error returning the excel files"
    RETURN_DATA_SQL_ERROR = "Error returning the sql files"
    RETURN_DATA_UNDEFINED_ERROR = "This file type is not defined!"
    NO_ERROR = "No error!"
    EXECUTE_QUERY_ERROR = "Error executing the query"
    ERROR_CREATE_STMNT = "Error with the create statement"
    ERROR_INSERT_STMNT = "Error inserting into sql from the dataframe"
    ERROR_CLOSING_CONN = "Error closing the database connection, check if connection exists"
    PREHOOK_SQL_ERROR = "Error executing sql files in the prehook"
    ERROR_CREATING_STAGING_TABLE = "Error creating the staging tables - prehook"
    HOOK_SQL_ERROR = "Error executing sql files in the hook"
    PANDAS_HANDLER_ERROR = "Error returning the cleaned dataframes!"
    TRUNCATE_ERROR = "Error truncating the staging tables!"
    EXECUTE_POSTHOOK_ERROR = "Error executing the posthook!"

class InputTypes (Enum):
    CSV = "csv"
    EXCEL = "excel"
    SQL = "sql"

class SQLCommands(Enum):
    SQL_FOLDER = "./SQL_Commands/"

class DestSchema(Enum):
    DW_SCHEMA = "dw_reporting"

class Sources(Enum):
    listings_source = "http://data.insideairbnb.com/united-states/ca/san-diego/2023-09-18/data/listings.csv.gz"
    reviews_source = "http://data.insideairbnb.com/united-states/ca/san-diego/2023-09-18/data/reviews.csv.gz"

class ETLStep(Enum):
    PRE_HOOK = "prehook"
    HOOK = "hook"

class PrehookSteps(Enum):
    EXECUTE_SQL_COMMANDS = "prehook_execute_sql_folder"
    CREATE_TABLE_IDX = "prehook_create_table_index"

class HookSteps (Enum):
    EXECUTE_SQL_COMMANDS = "hook_execute_sql_folder"
    CREATE_ETL_CHECKPOINT = "hook_create_etl_checkpoint"
    INSERT_UPDATE_ETL_CHECKPOINT = "hook_insert_or_update_etl_checkpoint"
    RETURN_LAST_ETL_RUN = "hook_return_last_etl_run"
    INSERT_INTO_STG_TABLE = "hook_insert_into_staging_tables"
    

class ETL_Checkpoint(Enum):
    TABLE = "etl_checkpoint"
    COLUMN = "etl_last_run"
    ETL_DEFAULT_DATE = "1900-01-01 00:00:00"

class StagingTables(Enum):
    STG_LISTING = "stg_cleaned_df1"
    STG_REVIEWS = "stg_cleaned_df2"