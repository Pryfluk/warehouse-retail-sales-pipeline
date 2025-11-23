import os
import pandas as pd
from sqlalchemy import create_engine

# --- Database connection ---
DB_HOST = "localhost"
DB_NAME = "mydatabase"
DB_USER = "postgres"
DB_PASS = "2548jewjew"
DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def transform_data(CSV_PATH: str, table_name: str, schema_name: str):
    """
    อ่านข้อมูล, ทำการแปลง, และเขียน DataFrame ที่แปลงแล้วกลับไปที่ PostgreSQL.
    """
    try:
        engine = create_engine(DATABASE_URL)
        print("เชื่อมต่อ PostgreSQL สำเร็จ")

        # อ่านข้อมูลจาก raw_data_table
        df = pd.read_sql_table('raw_data_table', con=engine, schema='raw_data')
        print("โหลดข้อมูลสำเร็จ")

        # --- Cleaning ---
        numeric_cols = ['RETAIL SALES', 'RETAIL TRANSFERS', 'WAREHOUSE SALES']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        df['YEAR'] = df['YEAR'].astype(str)
        df['MONTH'] = df['MONTH'].astype(str).str.zfill(2)
        df['DATE'] = pd.to_datetime(df['YEAR'] + '-' + df['MONTH'] + '-01')

        df['ITEM CODE'] = df['ITEM CODE'].astype(str)
        df['RETAIL SALES'] = df['RETAIL SALES'].astype(float)
        df['RETAIL TRANSFERS'] = df['RETAIL TRANSFERS'].astype(float)
        df['WAREHOUSE SALES'] = df['WAREHOUSE SALES'].astype(float)

        # Feature Engineering
        df['TOTAL_SALES_VOLUME'] = df['RETAIL SALES'] + df['WAREHOUSE SALES']
        df['TOTAL_MOVEMENT'] = df['RETAIL SALES'] + df['RETAIL TRANSFERS'] + df['WAREHOUSE SALES']
        df['SUPPLIER_CLEAN'] = df['SUPPLIER'].str.replace(r'\b(CO|INC|LLLP|LTD)\b', '', regex=True).str.strip()

        # --- Aggregation ---
        # รวม ITEM_DESCRIPTION ด้วยการเอา value แรกของแต่ละกลุ่ม
        df_monthly_summary = df.groupby(['DATE', 'SUPPLIER_CLEAN', 'ITEM TYPE']).agg(
            TOTAL_RETAIL_SALES=('RETAIL SALES', 'sum'),
            TOTAL_WAREHOUSE_SALES=('WAREHOUSE SALES', 'sum'),
            TOTAL_MOVEMENT_VOLUME=('TOTAL_MOVEMENT', 'sum'),
            NUMBER_OF_ITEMS=('ITEM CODE', 'nunique'),
            ITEM_DESCRIPTION=('ITEM DESCRIPTION', 'first')  # เอาค่ารายการแรกของแต่ละกลุ่ม
        ).reset_index()

        # --- เขียนกลับ PostgreSQL ---
        df_monthly_summary.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists='replace',
            index=False,
            method='multi'
        )

        print(f"เขียนข้อมูลเสร็จเรียบร้อยใน {schema_name}.{table_name}")

    except Exception as e:
        print(f"เกิดข้อผิดพลาด: {e}")


# --- Run script ---
if __name__ == "__main__":
    file_path = "data/Warehouse_and_Retail_Sales.csv"
    target_table = "monthly_sales_summary"
    target_schema = "production"

    if os.path.exists(file_path):
        transform_data(file_path, target_table, target_schema)
    else:
        print(f"ไม่พบไฟล์: {file_path}")
