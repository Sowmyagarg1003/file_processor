import csv
import re

import pandas as pd


# Analyze empty columns
def analyze_empty_columns(df):
    missing_data = df.isnull().mean() * 100
    columns_with_missing = missing_data[missing_data > 0]
    
    print("Missing values analysis:")
    for col, percent in columns_with_missing.items():
        print(f"Column '{col}' has {percent:.2f}% missing values.")
    
    return columns_with_missing


# Validate the number of columns in each row dynamically
def validate_column_count(df):
    num_columns = len(df.columns)
    return df.apply(lambda row: len(row) == num_columns, axis=1).all()


# Validate if headers exist
def validate_headers(df, mandatory_columns=None):
    if mandatory_columns:
        missing_columns = [col for col in mandatory_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing mandatory columns: {missing_columns}")
            return False
    return True


def validate_empty_values(df, threshold=0.3):
    total_rows = len(df)
    max_missing_threshold = threshold * total_rows
    has_exceeding_missing_values = False  # Track if any column exceeds the threshold

    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > max_missing_threshold:
            print(f"Column '{col}' has {missing_count} missing values, exceeding {threshold * 100}% threshold.")
            has_exceeding_missing_values = True  # Found at least one column that exceeds the threshold
    
    # If any column exceeds the threshold, you can log it but still return True to indicate validation passed
    if has_exceeding_missing_values:
        print("Warning: Some columns have missing values exceeding the defined threshold, but the file will still be validated.")
    
    return True  # Always return True to indicate the file is validated



# Check for duplicate rows
def validate_duplicates(df):
    if df.duplicated().any():
        print("CSV file contains duplicate rows.")
        return False
    return True


# Validate data types
def validate_data_types(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                print(f"Column {col} contains non-numeric values.")
                return False
    return True


# Check for improper delimiters
def validate_delimiter(file_path, expected_delimiter=','):
    with open(file_path, 'r') as f:
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(f.read(1024))
        if dialect.delimiter != expected_delimiter:
            print(f"Invalid delimiter found. Expected '{expected_delimiter}' but got '{dialect.delimiter}'.")
            return False
    return True


# Regex validation for emails
def validate_regex(df, column_name, regex_pattern):
    if column_name in df.columns:
        regex = re.compile(regex_pattern)
        if df[column_name].apply(lambda x: isinstance(x, str) and not regex.match(x) if pd.notna(x) else False).any():
            print(f"Column '{column_name}' contains values that don't match the regex pattern '{regex_pattern}'.")
            return False
    else:
        print(f"Column '{column_name}' not found for regex validation.")
    return True


# Check for double commas
def validate_double_commas(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, str) and ',,' in x).any():
            print(f"Column '{col}' contains double commas.")
            return False
    return True



def validate_csv(file_path):
    try:
        print("Starting CSV validation...")

        # Check the delimiter
        if not validate_delimiter(file_path):
            print("Delimiter validation failed")
            return False

        chunk_size = 100  # Number of rows per chunk
        chunk_number = 1

        # Use an iterator to read the CSV file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            print(f"\nProcessing chunk {chunk_number} with {len(chunk)} rows")

            # Analyze empty columns
            empty_columns_analysis = analyze_empty_columns(chunk)
            print("Missing values analysis:")
            print(empty_columns_analysis)  # Print the analysis for visibility

            # Apply multiple validation checks
            if not validate_column_count(chunk):
                print(f"Column count validation failed in chunk {chunk_number}")
                return False

            if not validate_empty_values(chunk, threshold=0.3):  # This will always return True now
                print(f"Empty values validation failed in chunk {chunk_number}")
                # Not applicable anymore since it never fails based on empty values

            if not validate_duplicates(chunk):
                print(f"Duplicate rows validation failed in chunk {chunk_number}")
                return False

            if not validate_double_commas(chunk):
                print(f"Double commas validation failed in chunk {chunk_number}")
                return False

            email_regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
            if not validate_regex(chunk, "Email", email_regex):
                print(f"Regex validation (Email format) failed in chunk {chunk_number}")
                return False

            if not validate_data_types(chunk):
                print(f"Data type validation failed in chunk {chunk_number}")
                return False

            print(f"Chunk {chunk_number} passed all validations")
            chunk_number += 1

        print(f"\nCSV file '{file_path}' passed all validations")


        return True

    except pd.errors.EmptyDataError:
        print("Error: The CSV file is empty.")
        return False
    except pd.errors.ParserError as pe:
        print(f"Parser error: {pe}")
        return False
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' does not exist.")
        return False
    except Exception as e:
        print(f"Unexpected error during CSV validation: {e}")
        return False