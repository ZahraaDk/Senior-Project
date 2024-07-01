import os 
from lookups import SQLCommands

def retrieve_sql_files(sql_command_path = SQLCommands.SQL_FOLDER):
    try:
        sql_files = [file for file in os.listdir(sql_command_path.value) if file.endswith('.sql')]
        return sorted(sql_files)
    except Exception as e:
        print(f"An error occured while retrieving sql files!: {e}")
