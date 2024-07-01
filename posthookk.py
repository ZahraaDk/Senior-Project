from database_handler import execute_query, create_connection, close_connection
from lookups import StagingTables, DestSchema, ErrorHandling
from logging_handler import show_error_message
import logging

def truncate_staging_table(db_session, source_name=DestSchema.DW_SCHEMA):
    try:
        table_names = [StagingTables.STG_LISTING, StagingTables.STG_REVIEWS]
        for table_name in table_names:
            dst_table = f"{source_name.value}.{table_name.value}"
            truncate_query = f"""
                TRUNCATE TABLE {dst_table}
            """
            execute_query(db_session, truncate_query)
            print(f"Table {dst_table} truncated successfully.")
        logging.info("Successfully truncated staging tables")
    except Exception as e:
        show_error_message(ErrorHandling.TRUNCATE_ERROR.value, str(e))


def execute_posthook():
    logging.info("Executing the posthook:")
    try:
        db_session = create_connection()
        truncate_staging_table(db_session, source_name = DestSchema.DW_SCHEMA)
        close_connection(db_session)
    except Exception as e:
        show_error_message(ErrorHandling.EXECUTE_POSTHOOK_ERROR.value, str(e))