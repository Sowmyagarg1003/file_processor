import csv
import os
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psycopg2
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

executor = ThreadPoolExecutor(max_workers=3)

# PostgreSQL connection
DB_CONFIG = {
    'dbname': 'file_processor',
    'user': 'postgres',
    'password': 'zxcvbnm',
    'host': '192.168.1.13',
    'port': '5432'
}

# Connection to PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Create table with dynamic columns based on the CSV headers
def create_table_from_csv(df, table_name='csv_data'):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Build the CREATE TABLE statement dynamically based on the CSV columns
    columns = df.columns
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    create_query += ', '.join([f'"{col}" TEXT' for col in columns])
    create_query += ', processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);'

    cursor.execute(create_query)
    conn.commit()
    conn.close()
    print(f"Table {table_name} created with columns: {', '.join(columns)}")

# Analyze empty columns
def analyze_empty_columns(df):
    missing_data = df.isnull().mean() * 100
    columns_with_missing = missing_data[missing_data > 0]

    if not columns_with_missing.empty:
        print("Columns with missing values (percentage of missing values):")
        print(columns_with_missing)
    else:
        print("No columns contain missing values.")

    return columns_with_missing

# Validate number of columns in each row dynamically
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
    
    return True

# Check for duplicate rows
def validate_duplicates(df):
    if df.duplicated().any():
        print("CSV file contains duplicate rows")
        return False
    return True

# Validate data types
def validate_data_types(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                print(f"Column {col} contains non-numeric values")
                return False
    return True

# Check for improper delimiters
def validate_delimiter(file_path, expected_delimiter=','):
    with open(file_path, 'r') as f:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(f.read(1024))
        if dialect.delimiter != expected_delimiter:
            print(f"Invalid delimiter found. Expected '{expected_delimiter}' but got '{dialect.delimiter}'")
            return False
    return True

# Regex validation for emails
def validate_regex(df, column_name, regex_pattern):
    if column_name in df.columns:
        regex = re.compile(regex_pattern)
        if df[column_name].apply(lambda x: isinstance(x, str) and not regex.match(x) if pd.notna(x) else False).any():
            print(f"Column {column_name} contains values that don't match the regex pattern {regex_pattern}")
            return False
    else:
        print(f"Skipping regex validation: Column '{column_name}' not found")
        return True

# Check for double commas
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

        # Email regex validation
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

# Insert the file name into PostgreSQL DB with retry mechanism
def insert_into_db(file_name):
    retries = 5
    for attempt in range(retries):
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # Insert the file name into the database
            insert_query = "INSERT INTO csv_data (file_name) VALUES (%s)"
            cursor.execute(insert_query, (file_name,))

            conn.commit()
            conn.close()
            break
        except psycopg2.OperationalError as e:
            print(f"Database is locked, retrying... (Attempt {attempt + 1})")
            time.sleep(5)

# Process file and insert data into the database
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

            # Create a table dynamically based on the CSV columns
            create_table_from_csv(df)

            # Insert data into the database
            conn = get_db_connection()
            cursor = conn.cursor()

            # Insert rows into the dynamically created table
            for _, row in df.iterrows():
                columns = ', '.join([f'"{col}"' for col in df.columns])
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO csv_data ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, tuple(row))

            conn.commit()
            conn.close()

            # Move the file to 'done' folder
            done_file_path = shutil.move(file_path, 'done')

            # Inserting file link to db
            print(f"Inserting file link for {done_file_path} into the database.")
            insert_into_db(os.path.basename(done_file_path))

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

# Retrieving file link from PostgreSQL DB
def retrieve_file_link(file_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Retrieving file name
        query = "SELECT file_name FROM csv_data WHERE file_name = %s"
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()

        conn.close()
        if result:
            return result[0]
    except Exception as e:
        print(f"Error retrieving file link: {e}")
    return None

# Watchdog event handler
class Watcher:
    def __init__(self, path='.'):
        self.observer = Observer()
        self.path = path

    def run(self):
        event_handler = Handler()
        self.observer.schedule(event_handler, self.path, recursive=True)
        self.observer.start()
        print("Monitoring started...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()

# File event handler
class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        executor.submit(process_file, event.src_path)

# Start the file processor
if __name__ == "__main__":
    watcher = Watcher(path='data/')
    watcher.run()
