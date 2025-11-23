import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import os
import time

# -----------------------------
# CONFIG
# -----------------------------
# ดึงค่าจาก Environment Variables หรือใช้ค่า Default
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "2548jewjew")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "mydatabase")
TABLE_NAME = os.environ.get("TABLE_NAME", "monthly_sales_summary")
SCHEMA_NAME = os.environ.get("SCHEMA_NAME", "production")

# Google Sheets Configuration
# NOTE: ตรวจสอบให้แน่ใจว่าไฟล์ service_account.json ถูกต้องและมีการแชร์สิทธิ์
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "data/aie321kaggleproject-91b110c2468f.json")
SHEET_NAME = os.environ.get("SHEET_NAME", "AIE321_Kaggle")
WORKSHEET_NAME = os.environ.get("WORKSHEET_NAME", "Production")

# สร้าง URL สำหรับการเชื่อมต่อฐานข้อมูล
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# สร้าง Engine สำหรับการเชื่อมต่อ PostgreSQL (ใช้ร่วมกันทั่วทั้งสคริปต์)
engine = create_engine(DATABASE_URL)

# -----------------------------
# PIPELINE READINESS CHECK
# ตรวจสอบว่าตารางปลายทาง (production.monthly_sales_summary) พร้อมใช้งานหรือไม่
# -----------------------------
max_retries = 10
retry_interval = 5  # วินาที

print(f"Checking for table '{SCHEMA_NAME}.{TABLE_NAME}' readiness...")

for i in range(max_retries):
    try:
        with engine.connect() as conn:
            # ลอง query ข้อมูลเพื่อยืนยันว่าตารางมีอยู่และสามารถเข้าถึงได้
            conn.execute(text(f"SELECT 1 FROM {SCHEMA_NAME}.{TABLE_NAME} LIMIT 1;"))
        print("PostgreSQL connection is ready, and target table exists.")
        break  # Table exists, ออกจาก loop และรันฟังก์ชัน publish
    except Exception as e:
        print(f"Table not ready, retrying ({i+1}/{max_retries})...")
        time.sleep(retry_interval)
else:
    # ถ้าพยายามจนครบแล้วแต่ยังไม่เจอ ให้หยุดการทำงาน
    raise Exception(f"Table {SCHEMA_NAME}.{TABLE_NAME} not found after retries. Pipeline halted.")

# -----------------------------
# FUNCTION
# -----------------------------
def publish_to_gsheet():
    """
    โหลด DataFrame จาก PostgreSQL และเขียนไปยัง Google Sheets
    """
    try:
        # --- Load DataFrame from DB ---
        # เนื่องจากเราทำการตรวจสอบความพร้อมของ DB และ Table ไปแล้ว
        # จึงสามารถใช้ engine โหลดข้อมูลได้ทันที
        df = pd.read_sql_table(TABLE_NAME, con=engine, schema=SCHEMA_NAME)
        print(f"Loaded data from DB. Shape = {df.shape}")

        # --- Connect to Google Sheets ---
        # gspread จะจัดการการตรวจสอบไฟล์ Service Account โดยใช้ path ที่กำหนด
        gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
        sh = gc.open(SHEET_NAME)

        try:
            # พยายามเปิด Worksheet ที่มีอยู่
            worksheet = sh.worksheet(WORKSHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            # ถ้าไม่พบ ให้สร้าง Worksheet ใหม่
            worksheet = sh.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")
            print(f"Worksheet '{WORKSHEET_NAME}' created.")

        # --- Write to Google Sheets ---
        set_with_dataframe(worksheet, df)
        print(f"Data published to Google Sheets '{SHEET_NAME}/{WORKSHEET_NAME}' successfully!")

    except Exception as e:
        print(f"Error during publishing: {e}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    publish_to_gsheet()