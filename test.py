import psycopg2

DB_CONFIG = {
    'dbname': 'file_processor',
    'user': 'postgres',
    'password': 'zxcvbnm',
    'host': '192.168.1.13',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("Connection successful!")
    conn.close()
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
