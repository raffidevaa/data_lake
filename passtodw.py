# passtodw.py

from sqlalchemy import text
from config.db_config import get_staging_engine, get_warehouse_engine
import pandas as pd

def load_table_from_staging_to_dw(table_name: str, schema_src: str, schema_dest: str):
    src_engine = get_staging_engine()
    dest_engine = get_warehouse_engine()

    # Baca dari staging
    query = f'SELECT * FROM {schema_src}.{table_name}'
    df = pd.read_sql(query, src_engine)

    # Tulis ke DW
    df.to_sql(table_name, dest_engine, schema=schema_dest, if_exists='replace', index=False)
    print(f"[SUCCESS] Loaded {len(df)} rows into {schema_dest}.{table_name}")

def passtodw():
    print("\n[INFO] Loading data from star_stg to data warehouse...")

    load_table_from_staging_to_dw('dim_word', 'star_stg', 'public')
    load_table_from_staging_to_dw('dim_source', 'star_stg', 'public')
    load_table_from_staging_to_dw('dim_time', 'star_stg', 'public')
    load_table_from_staging_to_dw('fact_wordcloud', 'star_stg', 'public')
    load_table_from_staging_to_dw('dim_sensor', 'star_stg', 'public')
    load_table_from_staging_to_dw('fact_temp_sensor', 'star_stg', 'public')
    load_table_from_staging_to_dw('dim_company', 'star_stg', 'public')
    load_table_from_staging_to_dw('fact_revenue', 'star_stg', 'public')

    print("\n[FINISHED] All tables loaded into DW.")

