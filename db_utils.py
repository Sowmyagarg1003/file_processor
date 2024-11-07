import pandas as pd
import psycopg2

from config import DB_CONFIG


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def create_table_from_csv(file_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    df_headers = pd.read_csv(file_path, nrows=1)
    create_query = f"CREATE TABLE IF NOT EXISTS {table_name} (" + ', '.join([f'"{col}" TEXT' for col in df_headers.columns]) + ');'
    cursor.execute(create_query)
    conn.commit()
    conn.close()

def insert_into_db(file_path, table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    chunk_size = 100
    total_rows_inserted = 0

    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        #insert statement for chunk
        columns = ', '.join([f'"{col}"' for col in chunk.columns])
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Converting chunk to list of tuples for insertion
        data = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
        
        # Execute batch insert
        cursor.executemany(insert_query, data)
        conn.commit()

        # Update the total rows inserted
        total_rows_inserted += len(data)

        # Calculate the range of rows inserted
        start_row = total_rows_inserted - len(data) + 1
        end_row = total_rows_inserted

        # Print the range of rows inserted
        print(f"Inserted rows {start_row} to {end_row} in {table_name}.")

    conn.close()
    print(f"All data from {file_path} inserted into table {table_name} in chunks of {chunk_size} rows.")



#def retrieve_file_link(table_name, file_name):
#    try:
#        conn = get_db_connection()
  #      cursor = conn.cursor()
#        query = f"SELECT file_name FROM {table_name} WHERE file_name = %s"
 #       cursor.execute(query, (file_name,))
#        result = cursor.fetchone()
#        conn.close()
 #       return result[0] if result else None
 #   except Exception as e:
 #       print(f"Error retrieving file link: {e}")
  #      return None#
