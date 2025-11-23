import os
import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2

# --- 1️⃣ Detect if running inside Docker ---
def is_docker():
    path = '/.dockerenv'
    if os.path.exists(path):
        return True
    try:
        with open('/proc/1/cgroup', 'rt') as f:
            return 'docker' in f.read() or 'kubepods' in f.read()
    except Exception:
        return False

# --- 2️⃣ Set DB_HOST depending on environment ---
if is_docker():
    DB_HOST = "postgres_db"      # service name in docker-compose
    CSV_PATH = "/data/Warehouse_and_Retail_Sales.csv"
    SERVICE_ACCOUNT_FILE = "/data/aie321kaggleproject-91b110c2468f.json"
else:
    DB_HOST = "localhost"        # host machine
    CSV_PATH = "data/Warehouse_and_Retail_Sales.csv"
    SERVICE_ACCOUNT_FILE = "data/aie321kaggleproject-91b110c2468f.json"

DB_PORT = "5432"
DB_USER = "postgres"
DB_PASS = "2548jewjew"
DB_NAME = "mydatabase"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def wait_for_db(host, port, user, password, db_name, retries=10, delay=3):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=db_name
            )
            conn.close()
            print("PostgreSQL ready!")
            return True
        except Exception:
            print(f"PostgreSQL not ready, retry {i+1}/{retries}...")
            time.sleep(delay)
    raise Exception("PostgreSQL not ready after retries")

# --- 3️⃣ Define functions for ingest, transform, publish ---
def ingest(csv_path):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df = pd.read_csv(csv_path)
    engine = create_engine(DATABASE_URL)
    df.to_sql("raw_data_table", engine, schema="raw_data", if_exists="replace", index=False)
    print("✅ Ingest completed.")

def transform():
    print("✅ Transform step executed (no action performed).")

def publish():
    import gspread
    from gspread_dataframe import set_with_dataframe

    engine = create_engine(DATABASE_URL)
    # อ่านจาก table เผื่อยังมีอยู่
    df = pd.read_sql_table("monthly_sales_summary", engine, schema="production")

    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    sh = gc.open("AIE321_Kaggle")

    try:
        worksheet = sh.worksheet("Production")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Production", rows="1000", cols="20")

    set_with_dataframe(worksheet, df)
    print("✅ Publish completed.")

# --- 4️⃣ Run full pipeline ---
if __name__ == "__main__":
    ingest(CSV_PATH)
    transform()
    publish()
    print("🎉 Pipeline finished successfully!")
