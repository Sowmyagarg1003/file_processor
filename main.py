import csv
import os
import re
import shutil
import psycopg2  # Replaced sqlite3 with psycopg2 for PostgreSQL
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

# Limit the number of concurrent threads
executor = ThreadPoolExecutor(max_workers=3)  # Reduced from 5 to 3

# PostgreSQL connection details
DB_CONFIG = {
    'dbname': 'file_processor',      # Replace with your database name
    'user': 'postgres',      # Replace with your username
    'password': 'zxcvbnm',         # Replace with your password
    'host': '192.168.1.13',                # Use 'localhost' if the DB is local
    'port': '5432'                      # Default PostgreSQL port
}

# Create a connection to PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Updated PostgreSQL DB creation to store the file link
def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS csv_data (
                        file_name TEXT,
                        file_path TEXT,
                        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.commit()
    conn.close()

# Function to analyze empty columns
def analyze_empty_columns(df):
    missing_data = df.isnull().mean() * 100
    columns_with_missing = missing_data[missing_data > 0]

    if not columns_with_missing.empty:
        print("Columns with missing values (percentage of missing values):")
        print(columns_with_missing)
    else:
        print("No columns contain missing values.")

    return columns_with_missing

# Validate the number of columns in each row dynamically
def validate_column_count(df):
    num_columns = len(df.columns)
    if df.apply(lambda row: len(row) == num_columns, axis=1).all():
        return True
    else:
        print("Inconsistent number of columns in the file")
        return False

# Validate if headers exist
def validate_headers(df, mandatory_columns=None):
    if mandatory_columns:
        missing_columns = [col for col in mandatory_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing mandatory columns: {missing_columns}")
            return False
    return True

# Check for empty values
def validate_empty_values(df, threshold=0.3):
    total_rows = len(df)
    max_missing_threshold = threshold * total_rows

    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > max_missing_threshold:
            print(f"Column '{col}' has {missing_count} missing values, exceeding {threshold*100}% threshold.")
            print(f"Column '{col}' is allowed to have more than {threshold*100}% missing values.")
    
    return True

# Check for duplicate rows
def validate_duplicates(df):
    if df.duplicated().any():
        print("CSV file contains duplicate rows")
        return False
    return True

# Validate data types (e.g., numeric columns should contain only numbers)
def validate_data_types(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                print(f"Column {col} contains non-numeric values")
                return False
    return True

# Check for improper delimiters in the file
def validate_delimiter(file_path, expected_delimiter=','):
    with open(file_path, 'r') as f:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(f.read(1024))
        if dialect.delimiter != expected_delimiter:
            print(f"Invalid delimiter found. Expected '{expected_delimiter}' but got '{dialect.delimiter}'")
            return False
    return True

def validate_regex(df, column_name, regex_pattern):
    if column_name in df.columns:
        regex = re.compile(regex_pattern)
        if df[column_name].apply(lambda x: isinstance(x, str) and not regex.match(x)).any():
            print(f"Column {column_name} contains values that don't match the regex pattern {regex_pattern}")
            return False
    else:
        print(f"Skipping regex validation: Column '{column_name}' not found")
        return True

# Check for double commas in the CSV file
def validate_double_commas(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, str) and ',,' in x).any():
            print(f"Column {col} contains double commas")
            return False
    return True

# Main validation function
def validate_csv(file_path):
    try:
        print("Starting CSV validation...")

        # Check the delimiter
        if not validate_delimiter(file_path):
            print("Delimiter validation failed")
            return False, None

        # Load the CSV file into a dataframe
        df = pd.read_csv(file_path)
        print(f"CSV file {file_path} loaded successfully")

        # Analyze empty columns
        analyze_empty_columns(df)

        # Apply multiple validation checks
        if not validate_column_count(df):
            print("Column count validation failed")
            return False, None

        # Validate empty values with a 30% threshold
        if not validate_empty_values(df, threshold=0.3):
            print("Empty values validation failed")
            return False, None

        if not validate_duplicates(df):
            print("Duplicate rows validation failed")
            return False, None

        if not validate_double_commas(df):
            print("Double commas validation failed")
            return False, None

        # Email regex validation (only if the 'Email' column exists)
        email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        if not validate_regex(df, "Email", email_regex):
            print("Regex validation (Email format) failed")
            return False, None

        # Data type validation
        if not validate_data_types(df):
            print("Data type validation failed")
            return False, None

        print(f"CSV file {file_path} passed all validations")
        return True, df

    except Exception as e:
        print(f"Error validating CSV: {e}")
        return False, None

# Insert the file path into PostgreSQL DB with retry mechanism
def insert_into_db(file_name, file_path):
    retries = 5
    for attempt in range(retries):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Insert the file name and path into the database
            insert_query = "INSERT INTO csv_data (file_name, file_path) VALUES (%s, %s)"
            cursor.execute(insert_query, (file_name, file_path))

            conn.commit()
            conn.close()
            break
        except psycopg2.OperationalError as e:
            print(f"Database is locked, retrying... (Attempt {attempt + 1})")
            time.sleep(5)  # Increased delay between retries

# Modify the process_file function to store the file path in the database
def process_file(file_path):
    print(f"Processing file: {file_path}")
    retries = 3
    for attempt in range(retries):
        try:
            time.sleep(5)

            # Validate CSV file
            is_valid, df = validate_csv(file_path)
            if not is_valid:
                print(f"Invalid CSV: {file_path}. Moving to error folder.")
                shutil.move(file_path, 'error')
                return {"data": [], "done": [], "error": [os.path.basename(file_path)], "folder": "error"}

            # Move the file to 'done' folder
            done_file_path = shutil.move(file_path, 'done')

            # Insert the file link into the database
            print(f"Inserting file link for {done_file_path} into the database.")
            insert_into_db(os.path.basename(done_file_path), done_file_path)

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

# Retrieve the file link from PostgreSQL DB
def retrieve_file_link(file_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query to retrieve the file path
        query = "SELECT file_path FROM csv_data WHERE file_name = %s"
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()

        conn.close()

        if result:
            return result[0]  # Returning the file path
        else:
            print("File not found in the database.")
            return None

    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return None

# Handle file detection
class Handler(FileSystemEventHandler):
    def process(self, event):
        if event.is_directory:
            return

        if event.event_type == 'created':
            print(f"New file detected: {event.src_path}")
            executor.submit(process_file, event.src_path)

    def on_created(self, event):
        self.process(event)

# Main function to start observing the directory
if __name__ == "__main__":
    create_table()  # Ensure the table exists
    path = 'data'  # Directory to watch
    observer = Observer()
    event_handler = Handler()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    print("Monitoring started...")

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
