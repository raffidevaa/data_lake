# transform.py

import pandas as pd
from sqlalchemy import text
from config.db_config import get_staging_engine

def transform_txt():
    engine = get_staging_engine()

    with engine.begin() as conn:
        # Buat schema dan tabel dimensi + fakta
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS star_stg;

            CREATE TABLE IF NOT EXISTS star_stg.dim_word (
                word_key SERIAL PRIMARY KEY,
                word TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS star_stg.dim_source (
                source_key SERIAL PRIMARY KEY,
                source_name TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS star_stg.fact_wordcloud (
                id SERIAL PRIMARY KEY,
                word_key INT REFERENCES star_stg.dim_word(word_key),
                source_key INT REFERENCES star_stg.dim_source(source_key),
                frequency INT
            );
        """))

        # Ambil data dari staging
        df = pd.read_sql("SELECT word, frequency, source FROM txt_stg.words_stg", conn)
        if df.empty:
            print("[WARNING] Tidak ada data di staging untuk ditransformasi.")
            return

        # === DIMENSI KATA ===
        word_df = df[['word']].drop_duplicates()
        word_df.to_sql("dim_word", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        dim_word = pd.read_sql("SELECT * FROM star_stg.dim_word", engine)
        df = df.merge(dim_word, on='word', how='left')

        # === DIMENSI SOURCE ===
        source_df = df[['source']].drop_duplicates().rename(columns={'source': 'source_name'})
        source_df.to_sql("dim_source", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        dim_source = pd.read_sql("SELECT * FROM star_stg.dim_source", engine)
        df = df.merge(dim_source, left_on='source', right_on='source_name', how='left')

        # === FAKTA ===
        fact_df = df[['word_key', 'source_key', 'frequency']]
        fact_df.to_sql("fact_wordcloud", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        print(f"[INFO] Transformasi selesai: {len(fact_df)} baris dimuat ke star_stg.fact_wordcloud.")

def transform_csv():
    engine = get_staging_engine()

    with engine.begin() as conn:
        # Create schema dan tabel star_stg bila belum ada
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS star_stg;

            CREATE TABLE IF NOT EXISTS star_stg.dim_time (
                time_key SERIAL PRIMARY KEY,
                year TEXT,
                month TEXT,
                day TEXT,
                UNIQUE (year, month, day)
            );

            CREATE TABLE IF NOT EXISTS star_stg.dim_sensor (
                sensor_key SERIAL PRIMARY KEY,
                sensor_name TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS star_stg.fact_temp_sensor (
                id SERIAL PRIMARY KEY,
                time_key INT REFERENCES star_stg.dim_time(time_key),
                sensor_key INT REFERENCES star_stg.dim_sensor(sensor_key),
                min_temp NUMERIC(5,2),
                max_temp NUMERIC(5,2),
                avg_temp NUMERIC(5,2)
            );
        """))

        # Ambil data dari staging csv_stg.temp_sensor_stg
        df = pd.read_sql("SELECT date, min_temp, max_temp, avg_temp FROM csv_stg.temp_sensor_stg", conn)
        if df.empty:
            print("[WARNING] Tidak ada data di staging untuk ditransformasi.")
            return

        # === DIMENSI WAKTU ===
        df['date'] = pd.to_datetime(df['date'])
        time_df = (
            df[['date']]
            .drop_duplicates()
            .assign(
                year=lambda x: x['date'].dt.year.astype(str),
                month=lambda x: x['date'].dt.month.astype(str),
                day=lambda x: x['date'].dt.day.astype(str)
            )
            [['year', 'month', 'day']]
            .drop_duplicates()
        )
        time_df.to_sql("dim_time", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        # Ambil kembali dim_time untuk join
        dim_time = pd.read_sql("SELECT * FROM star_stg.dim_time", engine)
        df['merge_date'] = df['date'].dt.date
        dim_time['merge_date'] = pd.to_datetime(
            dim_time[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1)
        ).dt.date
        df = df.merge(dim_time[['time_key', 'merge_date']], on='merge_date', how='left')

        # === DIMENSI SENSOR ===
        sensor_df = pd.DataFrame({'sensor_name': ['Warehouse Temperature Sensor']})
        sensor_df.to_sql("dim_sensor", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        # Ambil kembali dim_sensor
        dim_sensor = pd.read_sql("SELECT * FROM star_stg.dim_sensor", engine)
        sensor_key = dim_sensor.loc[dim_sensor['sensor_name'] == 'Warehouse Temperature Sensor', 'sensor_key'].values[0]

        # Tambahkan sensor_key ke dataframe
        df['sensor_key'] = sensor_key

        # === FAKTA ===
        fact_df = df[['time_key', 'sensor_key', 'min_temp', 'max_temp', 'avg_temp']]
        fact_df.to_sql("fact_temp_sensor", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        print(f"[INFO] Transformasi selesai: {len(fact_df)} baris dimuat ke star_stg.fact_temp_sensor.")

def transform_pdf():
    engine = get_staging_engine()

    with engine.begin() as conn:
        # Pastikan semua tabel schema star_stg sudah ada
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS star_stg;

            CREATE TABLE IF NOT EXISTS star_stg.dim_time (
                time_key SERIAL PRIMARY KEY,
                year TEXT,
                month TEXT,
                day TEXT,
                UNIQUE (year, month, day)
            );

            CREATE TABLE IF NOT EXISTS star_stg.dim_company (
                company_key SERIAL PRIMARY KEY,
                company_name TEXT UNIQUE
            );

            CREATE TABLE IF NOT EXISTS star_stg.fact_revenue (
                id SERIAL PRIMARY KEY,
                company_key INT REFERENCES star_stg.dim_company(company_key),
                time_key INT REFERENCES star_stg.dim_time(time_key),
                revenue NUMERIC
            );
        """))

        # Ambil data dari staging
        df = pd.read_sql("SELECT company_name, report_date, revenue FROM pdf_stg.revenue_stg", conn)
        if df.empty:
            print("[WARNING] Tidak ada data di staging pdf untuk ditransformasi.")
            return

        # === DIMENSI WAKTU ===
        df['report_date'] = pd.to_datetime(df['report_date'])
        time_df = (
            df[['report_date']]
            .drop_duplicates()
            .assign(
                year=lambda x: x['report_date'].dt.year.astype(str),
                month=lambda x: x['report_date'].dt.month.astype(str),
                day=lambda x: x['report_date'].dt.day.astype(str)
            )
            [['year', 'month', 'day']]
            .drop_duplicates()
        )
        time_df.to_sql("dim_time", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        # Ambil kembali dim_time untuk join
        dim_time = pd.read_sql("SELECT * FROM star_stg.dim_time", engine)
        df['merge_date'] = df['report_date'].dt.date
        dim_time['merge_date'] = pd.to_datetime(
            dim_time[['year', 'month', 'day']].astype(str).agg('-'.join, axis=1)
        ).dt.date
        df = df.merge(dim_time[['time_key', 'merge_date']], on='merge_date', how='left')

        # === DIMENSI COMPANY ===
        company_df = df[['company_name']].drop_duplicates()
        company_df.to_sql("dim_company", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        # Ambil kembali dim_company untuk join
        dim_company = pd.read_sql("SELECT * FROM star_stg.dim_company", engine)
        df = df.merge(dim_company, on='company_name', how='left')

        # === FAKTA REVENUE ===
        fact_df = df[['company_key', 'time_key', 'revenue']]
        fact_df.to_sql("fact_revenue", con=engine, schema="star_stg", if_exists="append", index=False, method="multi")

        print(f"[INFO] Transformasi selesai: {len(fact_df)} baris dimuat ke star_stg.fact_revenue.")