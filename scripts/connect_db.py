import psycopg2

DB_NAME = "hq_warehouse"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"

def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None