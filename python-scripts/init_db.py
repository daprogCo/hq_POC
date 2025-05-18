import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

DB_NAME = "hq_warehouse"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="postgres",  # se connecter à la base postgres pour créer une autre base
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

# Création de la base si elle n'existe pas
cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
exists = cursor.fetchone()

if not exists:
    print(f"Creating database '{DB_NAME}'...")
    cursor.execute(f"CREATE DATABASE {DB_NAME}")
else:
    print(f"Database '{DB_NAME}' already exists.")

cursor.close()
conn.close()