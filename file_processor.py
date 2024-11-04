import os
import shutil
import time
from db_utils import insert_into_db
from csv_validator import validate_csv

def process_file(file_path):
    print(f"Processing file: {file_path}")
    try:
        time.sleep(5)

        # Validate CSV file
        is_valid, df = validate_csv(file_path)
        if not is_valid:
            print(f"Invalid CSV: {file_path}. Moving to error folder.")
            shutil.move(file_path, 'error')
            return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}

        # Create a table dynamically based on CSV columns
        create_table_from_csv(file_path)

        # Insert data into the created table
        insert_csv_data_into_db(file_path)

        # Move the file to 'done' folder
        done_file_path = shutil.move(file_path, 'done')
        print(f"File {os.path.basename(file_path)} processed successfully and moved to 'done'!")
        return {"data": [], "done": [os.path.basename(done_file_path)], "error": [], "folder": "done"}

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        if os.path.exists(file_path):
            shutil.move(file_path, 'error')
            print(f"File {file_path} moved to error folder due to an error.")
        return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}
