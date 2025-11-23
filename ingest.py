import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

CSV_PATH = "data/Warehouse_and_Retail_Sales.csv"

DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "2548jewjew")
DB_HOST = os.environ.get("DB_HOST", "localhost") 
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "mydatabase")

POSTGRES_CONN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

TABLE_NAME = "raw_data_table"

# -----------------------------
# Load CSV → DataFrame
# -----------------------------
def load_csv():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded CSV. Shape = {df.shape}")
    return df

# -----------------------------
# Load DataFrame → PostgreSQL
# -----------------------------
def load_to_postgres(df, max_retries=10, wait_seconds=3):
    engine = create_engine(POSTGRES_CONN)

    # Retry logic รอ DB พร้อม
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Connected to PostgreSQL successfully!")
            break
        except OperationalError:
            print(f"PostgreSQL not ready, retrying ({attempt+1}/{max_retries})...")
            time.sleep(wait_seconds)
    else:
        raise Exception("Could not connect to PostgreSQL after multiple retries.")

    # สร้าง schema raw_data ถ้ายังไม่มี
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data;"))
        print("Schema raw_data ready.")

    # โหลด DataFrame เข้า table
    df.to_sql(
        TABLE_NAME,
        con=engine,
        schema="raw_data",
        if_exists="replace",  # เขียนทับทุกครั้ง
        index=False
    )

    print(f"Loaded DataFrame into raw_data.{TABLE_NAME}")

# -----------------------------
# MAIN
# -----------------------------
def run_ingest():
    df = load_csv()
    load_to_postgres(df)
    print("Ingestion completed successfully!")

if __name__ == "__main__":
    run_ingest()
