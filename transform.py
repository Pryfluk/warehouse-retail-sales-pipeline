import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import os

# --- 1. การกำหนดค่าการเชื่อมต่อฐานข้อมูล (PostgreSQL) ---
# NOTE: คุณจะต้องแทนที่ค่าเหล่านี้ด้วยข้อมูลการเชื่อมต่อ PostgreSQL ของคุณ
DB_HOST = "localhost"
DB_NAME = "mydatabase"
DB_USER = "postgres"
DB_PASS = "2548jewjew"
DB_PORT = "5432"

# สร้าง URL สำหรับการเชื่อมต่อ
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 2. ฟังก์ชันหลักในการโหลด, แปลง, และบันทึกข้อมูล ---

def transform_data(CSV_PATH: str, table_name: str, schema_name: str):
    """
    อ่านข้อมูล, ทำการแปลง, และเขียน DataFrame ที่แปลงแล้วกลับไปที่ PostgreSQL.
    """
    try:
        # --- 2.1 อ่านข้อมูลเข้าสู่ DataFrame (สมมติว่าอ่านจากไฟล์ CSV ที่ให้มา) ---
        engine = create_engine(DATABASE_URL)
        print(f"กำลังเชื่อมต่อและอ่านข้อมูลจากตาราง 'raw_data.raw_data_table'...")
        
        df = pd.read_sql_table('raw_data_table', con=engine, schema='raw_data')
        print("โหลดข้อมูลสำเร็จ.")

        # --- 2.2 Cleaning: จัดการค่า Missing Values และ Data Type ที่ไม่ถูกต้อง ---

        print("กำลังทำความสะอาดข้อมูล...")
        
        # 1. จัดการ Missing Values (ในข้อมูลตัวอย่างไม่มีค่า NaN ชัดเจน แต่ควรทำเผื่อ)
        # เติม 0 ให้กับคอลัมน์การขาย/โอนย้ายที่เป็นตัวเลข
        numeric_cols = ['RETAIL SALES', 'RETAIL TRANSFERS', 'WAREHOUSE SALES']
        for col in numeric_cols:
            if df[col].dtype == 'object':
                 # แปลงเป็นตัวเลขก่อน, บังคับให้เกิด NaN ถ้าค่าไม่ถูกต้อง
                df[col] = pd.to_numeric(df[col], errors='coerce') 
            df[col] = df[col].fillna(0) # เติมค่าว่างด้วย 0
        
        # 2. จัดการ Data Type 
        df['YEAR'] = df['YEAR'].astype(str)
        df['MONTH'] = df['MONTH'].astype(str).str.zfill(2) # เพิ่ม 0 นำหน้าเดือนเดียว
        
        # สร้างคอลัมน์ Date (รวม YEAR และ MONTH)
        df['DATE'] = pd.to_datetime(df['YEAR'] + '-' + df['MONTH'] + '-01')
        
        # ตั้งค่า Data Type ที่เหลือให้เหมาะสม
        df['ITEM CODE'] = df['ITEM CODE'].astype(str)
        df['RETAIL SALES'] = df['RETAIL SALES'].astype(float)
        df['RETAIL TRANSFERS'] = df['RETAIL TRANSFERS'].astype(float)
        df['WAREHOUSE SALES'] = df['WAREHOUSE SALES'].astype(float)
        
        # --- 2.3 Transformation: สร้างคอลัมน์ใหม่ที่จำเป็น (Feature Engineering) ---
        
        # 1. ยอดขายรวมทั้งหมด (Total Sales Volume)
        df['TOTAL_SALES_VOLUME'] = df['RETAIL SALES'] + df['WAREHOUSE SALES']
        
        # 2. ยอดเคลื่อนไหวรวม (Total Movement: Sales + Transfers)
        df['TOTAL_MOVEMENT'] = df['RETAIL SALES'] + df['RETAIL TRANSFERS'] + df['WAREHOUSE SALES']
        
        # 3. จัดประเภท Supplier ให้สั้นลง (ตัวอย่าง: ตัดคำที่ซ้ำซ้อน)
        df['SUPPLIER_CLEAN'] = df['SUPPLIER'].str.replace(r'\b(CO|INC|LLLP|LTD)\b', '', regex=True).str.strip()
        
        print("ทำ Transformation และ Feature Engineering สำเร็จ.")
        
        # --- 2.4 Aggregation: สรุปข้อมูล (เช่น ยอดขายรายเดือน) ---
        
        # สรุปยอดขายรวมรายเดือน (Total Monthly Sales)
        df_monthly_summary = df.groupby(['DATE', 'SUPPLIER_CLEAN', 'ITEM TYPE']).agg(
            TOTAL_RETAIL_SALES=('RETAIL SALES', 'sum'),
            TOTAL_WAREHOUSE_SALES=('WAREHOUSE SALES', 'sum'),
            TOTAL_MOVEMENT_VOLUME=('TOTAL_MOVEMENT', 'sum'),
            NUMBER_OF_ITEMS=('ITEM CODE', 'nunique') # นับจำนวนรหัสสินค้าที่ไม่ซ้ำกัน
        ).reset_index()
        
        df_final = df_monthly_summary
        
        print("ทำ Aggregation สำเร็จ.")

        # --- 2.5 Joining: (ไม่จำเป็น เนื่องจากมีไฟล์เดียว) ---

        # ในกรณีที่มีหลายไฟล์ Kaggle, จะใช้ pd.merge() ณ ขั้นตอนนี้

        # --- 3. เขียน DataFrame ที่แปลงแล้วกลับไปเป็นตารางใหม่ใน Schema production ---

        print(f"กำลังเขียนข้อมูลไปยังตาราง {schema_name}.{table_name} ใน PostgreSQL...")
        
        engine = create_engine(DATABASE_URL)
        
        # เขียน DataFrame ลงใน PostgreSQL
        # if_exists='replace' คือการลบตารางเดิมและสร้างใหม่
        # index=False คือไม่รวม index ของ DataFrame เป็นคอลัมน์ในตาราง
        df_final.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='replace',
            index=False,
            method='multi' # ใช้วิธี multi เพื่อประสิทธิภาพที่ดีขึ้นในการเขียนข้อมูลขนาดใหญ่
        )
        
        print(f"การแปลงข้อมูลสำเร็จและถูกบันทึกไปยังตาราง '{schema_name}.{table_name}' เรียบร้อยแล้ว.")

    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {e}")

# --- 4. การรันสคริปต์ ---
if __name__ == "__main__":
    # ตรวจสอบว่าไฟล์ CSV อยู่ในไดเรกทอรีเดียวกันหรือไม่
    file_path = "data/Warehouse_and_Retail_Sales.csv" 
    target_table = "monthly_sales_summary" # ชื่อตารางปลายทาง
    target_schema = "production" # Schema ปลายทาง

    if os.path.exists(file_path):
        transform_data(file_path, target_table, target_schema)
    else:
        print(f"ไม่พบไฟล์: {file_path}. โปรดตรวจสอบว่าไฟล์อยู่ในไดเรกทอรีที่ถูกต้อง.")