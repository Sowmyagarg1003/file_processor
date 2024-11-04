import psycopg2
import pandas as pd

DB_CONFIG = {
    'dbname': 'file_processor',
    'user': 'postgres',
    'password': 'zxcvbnm',
    'host': '192.168.1.13',
    'port': '5432'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Create table with dynamic columns based on the CSV structure
def create_table_from_csv(file_path, table_name='csv_data'):
    conn = get_db_connection()
    cursor = conn.cursor()

    # Read the CSV file to get the columns
    df = pd.read_csv(file_path)
    columns = df.columns

    # Dynamically build the CREATE TABLE query
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    create_query += ', '.join([f'"{col}" TEXT' for col in columns])
    create_query += ', processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);'

    cursor.execute(create_query)
    conn.commit()
    conn.close()
    print(f"Table {table_name} created with columns: {', '.join(columns)}")


# Insert CSV data into the dynamically created table
def insert_csv_data_into_db(file_path, table_name='csv_data'):
    df = pd.read_csv(file_path)
    
    conn = get_db_connection()
    cursor = conn.cursor()

    # Convert dataframe to tuples and prepare INSERT query
    columns = ', '.join([f'"{col}"' for col in df.columns])
    placeholders = ', '.join(['%s' for _ in df.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    try:
        for _, row in df.iterrows():
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        print(f"Data from {file_path} inserted successfully into {table_name}.")
    except psycopg2.Error as e:
        print(f"Database insertion error: {e}")
    finally:
        conn.close()


def retrieve_file_link(file_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT file_path FROM csv_data WHERE file_name = %s"
        cursor.execute(query, (file_name,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        return None
