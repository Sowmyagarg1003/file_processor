import pandas as pd
import re
import csv

def analyze_empty_columns(df):
    missing_data = df.isnull().mean() * 100
    columns_with_missing = missing_data[missing_data > 0]
    if not columns_with_missing.empty:
        print("Columns with missing values (percentage of missing values):")
        print(columns_with_missing)
    else:
        print("No columns contain missing values.")
    return columns_with_missing

def validate_column_count(df):
    num_columns = len(df.columns)
    if df.apply(lambda row: len(row) == num_columns, axis=1).all():
        return True
    else:
        print("Inconsistent number of columns in the file")
        return False

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
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count > max_missing_threshold:
            print(f"Column '{col}' has {missing_count} missing values, exceeding {threshold*100}% threshold.")
    return True

def validate_duplicates(df):
    if df.duplicated().any():
        print("CSV file contains duplicate rows")
        return False
    return True

def validate_data_types(df):
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            if not pd.to_numeric(df[col], errors='coerce').notnull().all():
                print(f"Column {col} contains non-numeric values")
                return False
    return True

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
        if df[column_name].apply(lambda x: isinstance(x, str) and not regex.match(x) if pd.notna(x) else False).any():
            print(f"Column {column_name} contains values that don't match the regex pattern {regex_pattern}")
            return False
    else:
        print(f"Skipping regex validation: Column '{column_name}' not found")
        return True

def validate_double_commas(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, str) and ',,' in x).any():
            print(f"Column {col} contains double commas")
            return False
    return True
