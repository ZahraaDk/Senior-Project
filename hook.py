from misc_handler import retrieve_sql_files
from lookups import ErrorHandling, DestSchema, SQLCommands, HookSteps, ETL_Checkpoint, InputTypes
from database_handler import execute_query, read_data_as_dataframe, insert_into_sql_statement_from_df, create_connection, close_connection
from logging_handler import show_error_message
import os 
import datetime
from pandas_handler import dataframes_cleansed
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging

def execute_sql_folder_hook(db_session, target_schema = DestSchema.DW_SCHEMA, sql_directory_path = SQLCommands.SQL_FOLDER):
    sql_files = None
    try:
        sql_files = retrieve_sql_files(sql_directory_path)
        for sql_file in sql_files:
            if 'hook' in sql_file:
                with open(os.path.join(sql_directory_path.value,sql_file), 'r') as file:
                    sql_query = file.read() 
                    sql_query = sql_query.replace('target_schema', target_schema.value)
                    return_query = execute_query(db_session=db_session, query=sql_query)
                    if not return_query == ErrorHandling.NO_ERROR:
                        raise Exception( f" {HookSteps.EXECUTE_SQL_COMMANDS.value} = SQL File Error on SQL file = " + str(sql_file))
        logging.info("Hook SQL Folder was successfully executed!")
        logging.info("Data was successfully inserted into Relational database(PostgreSQL)")
    except Exception as e:
        show_error_message(ErrorHandling.HOOK_SQL_ERROR.value, str(e))
    finally:
        return sql_files  

def create_etl_checkpoint(target_schema, db_session):
    query = None
    try:
        query = f"""CREATE TABLE IF NOT EXISTS {target_schema.value}.{ETL_Checkpoint.TABLE.value}
        (
            {ETL_Checkpoint.COLUMN.value} TIMESTAMP
        );
        """
        execute_query(db_session, query)
        logging.info("ETL Checkpoint was created!")
    except Exception as e:
        show_error_message(HookSteps.CREATE_ETL_CHECKPOINT.value,str(e))
    finally:
        return query
    
def insert_or_update_etl_checkpoint(db_session,
                                    etl_time_exists = False,
                                    target_schema = DestSchema.DW_SCHEMA,
                                    table_name = ETL_Checkpoint.TABLE,
                                    column_name = ETL_Checkpoint.COLUMN):
    try: 
        if etl_time_exists:
            update_query = f"""
                UPDATE {target_schema.value}.{table_name.value}
                SET {column_name.value} = '{datetime.datetime.now()}'
            """
            execute_query(db_session=db_session, query=update_query)
        else:
            insert_query = f"""
                INSERT INTO {target_schema.value}.{table_name.value}
                VALUES('{ETL_Checkpoint.ETL_DEFAULT_DATE.value}')
            """
            execute_query(db_session=db_session, query=insert_query)
        logging.info("Updated ETL last_update!")
    except Exception as e:
        show_error_message(HookSteps.INSERT_UPDATE_ETL_CHECKPOINT.value,str(e))



def return_etl_last_updated_date(db_session,
                                target_schema = DestSchema.DW_SCHEMA,
                                etl_date = ETL_Checkpoint.ETL_DEFAULT_DATE,
                                table_name = ETL_Checkpoint.TABLE,
                                column_name = ETL_Checkpoint.COLUMN):
    etl_time_exists = False
    return_date = None
    try:
        query = f"SELECT {column_name.value} FROM {target_schema.value}.{table_name.value} ORDER BY {column_name.value} DESC LIMIT 1"
        etl_df = read_data_as_dataframe(file_type = InputTypes.SQL, file_path = query, db_session= db_session)
        if len(etl_df) == 0:
            return_date = etl_date.value
        else:
            return_date = etl_df[column_name.value].iloc[0]
            etl_time_exists = True
        logging.info("ETL last updated date was returned.")
    except Exception as e:
        show_error_message(HookSteps.RETURN_LAST_ETL_RUN.value, str(e))
    finally:    
        return return_date, etl_time_exists
    
sia = SentimentIntensityAnalyzer()
def analyze_sentiment(text):
    sentiment = sia.polarity_scores(text)
    compound_score = sentiment['compound']
    return compound_score

def categorize_sentiment(sentiment_score):
    if sentiment_score > 0.1:
        return "Positive"
    elif sentiment_score < -0.1:
        return "Negative"
    else:
        return "Neutral"

def apply_sentiment_analysis(df):
    df['sentiment_score'] = df['comments'].apply(analyze_sentiment)
    df['sentiment'] = df['sentiment_score'].apply(categorize_sentiment)

def insert_into_stg_tables(db_session, target_schema=DestSchema.DW_SCHEMA, etl_date=None):
    messages = []
    dataframes_dict = dataframes_cleansed()
    try:
        for table_name, staging_df in dataframes_dict.items():
            if 'comments' in staging_df.columns:
                query = f"""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name = 'stg_{table_name}'
                            AND column_name = 'sentiment_score'
                        )
                        THEN
                            ALTER TABLE {target_schema.value}.stg_{table_name}
                            ADD COLUMN sentiment_score DECIMAL(10, 5);
                        END IF;
                        IF NOT EXISTS (
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_name = 'stg_{table_name}'
                            AND column_name = 'sentiment'
                        )
                        THEN 
                            ALTER TABLE {target_schema.value}.stg_{table_name}
                            ADD COLUMN sentiment VARCHAR(255);
                        END IF;
                    END $$;

                """
                execute_query(db_session, query)
                apply_sentiment_analysis(staging_df)
                logging.info("Sentiment Analysis was applied!")

            staging_dfs = staging_df[staging_df['booking_date'] > etl_date]
            if not staging_dfs.empty:
                insert_stmt = insert_into_sql_statement_from_df(staging_dfs, target_schema.value, f"stg_{table_name}")
                execute_return = execute_query(db_session=db_session, query=insert_stmt)
                print("Data was successfully inserted into staging tables.")
                if execute_return != ErrorHandling.NO_ERROR:
                    raise Exception(f"Error inserting data into stg_{table_name}: {execute_return}")
                messages.append(f"Inserted new data after '{etl_date}' into stg_{table_name} successfully.")
        logging.info("Successfully inserted new data into the staging tables!")
    except Exception as e:
        show_error_message(HookSteps.INSERT_INTO_STG_TABLE.value, str(e))
    return messages

def execute_hook():
    logging.info("Executing the Hook:")
    step = None
    try:
        db_session = create_connection()
        create_etl_checkpoint(DestSchema.DW_SCHEMA,db_session)
        etl_date, etl_time_exists = return_etl_last_updated_date(db_session)
        insert_into_stg_tables(db_session,DestSchema.DW_SCHEMA, etl_date)
        execute_sql_folder_hook(db_session)
        insert_or_update_etl_checkpoint(db_session, etl_time_exists=etl_time_exists)
        close_connection(db_session)
    except Exception as e:
        error_prefix = f'{ErrorHandling.HOOK_SQL_ERROR.value}'
        show_error_message(error_prefix,str(e))