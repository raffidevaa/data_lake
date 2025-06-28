import os
import pandas as pd
from collections import Counter
from config.db_config import get_staging_engine
from sqlalchemy import text
from datetime import datetime

def load_txt_to_staging(structured_txt_path="structured/txt/words_freq.csv"):
    if not os.path.exists(structured_txt_path):
        print(f"[ERROR] File tidak ditemukan: {structured_txt_path}")
        return

    df = pd.read_csv(structured_txt_path)
    if df.empty:
        print("[WARNING] File kosong, tidak ada data yang dimuat.")
        return

    df["source"] = "X"

    df["insert_at"] = datetime.now()

    engine = get_staging_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS txt_stg;
            CREATE TABLE IF NOT EXISTS txt_stg.words_stg (
                id SERIAL PRIMARY KEY,
                word TEXT,
                frequency INT,
                source TEXT,
                insert_at TIMESTAMP
            );
        """))

    df.to_sql("words_stg", engine, schema="txt_stg", if_exists="replace", index=False)
    print(f"[INFO] Berhasil dimuat ke staging DB ({len(df)} baris).")


def load_csv_to_staging(input_folder="structured/csv"):
    engine = get_staging_engine()

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            print(f"[INFO] Loading file: {file_path}")

            df = pd.read_csv(file_path)
            df["insert_at"] = datetime.now()

            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE SCHEMA IF NOT EXISTS csv_stg;
                    CREATE TABLE IF NOT EXISTS csv_stg.temp_sensor_stg (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        value NUMERIC,
                        insert_at TIMESTAMP
                    );
                """))

            df.to_sql(
                name="temp_sensor_stg",
                con=engine,
                schema="csv_stg",
                if_exists="append",
                index=False
            )

            print(f"[SUCCESS] {len(df)} rows loaded from {filename} into csv_stg.temp_sensor_stg")


def load_pdf_to_staging():
    engine = get_staging_engine()

    input_file = "structured/pdf/structured_revenue.csv"
    df = pd.read_csv(input_file)
    df['report_date'] = pd.to_datetime(df['report_date']).dt.date
    df["insert_at"] = datetime.now()

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS pdf_stg;
            CREATE TABLE IF NOT EXISTS pdf_stg.revenue_stg (
                id SERIAL PRIMARY KEY,
                company_name TEXT,
                report_date DATE,
                revenue NUMERIC,
                insert_at TIMESTAMP
            );
        """))

    df.to_sql(
        "revenue_stg",
        con=engine,
        schema="pdf_stg",
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"[INFO] {len(df)} record berhasil dimuat ke pdf_stg.revenue_stg")
