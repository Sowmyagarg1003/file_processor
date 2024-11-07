import os
import shutil
import time

from db_utils import create_table_from_csv, insert_into_db
from file_validator import validate_csv


def process_file(file_path):
    print(f"Processing file: {file_path}")
    retries = 3
    for attempt in range(retries):
        try:
            time.sleep(5)

            # Validate CSV file
            is_valid = validate_csv(file_path)
            if not is_valid:
                print(f"Invalid CSV: {file_path}. Moving to error folder.")
                shutil.move(file_path, 'error')
                return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}

            #Get the file name without its extension
            table_name = os.path.splitext(os.path.basename(file_path))[0]

            # Creating table dynamically based on the CSV columns
            create_table_from_csv(file_path, table_name)

            # Insert data into the database
            insert_into_db(file_path, table_name)

            # Movint the file to done folder
            done_file_path = shutil.move(file_path, 'done')
            print(f"File {os.path.basename(file_path)} processed successfully and moved to 'done'!")
            return {"data": [], "done": [os.path.basename(done_file_path)], "error": [], "folder": "done"}

        except PermissionError as e:
            print(f"PermissionError on attempt {attempt + 1}: {e}")
            time.sleep(10)

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            if os.path.exists(file_path):
                shutil.move(file_path, 'error')
                print(f"File {file_path} moved to error folder due to an error.")
            return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}
