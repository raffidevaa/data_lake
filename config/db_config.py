# config/db_config.py

from sqlalchemy import create_engine

# Engine untuk koneksi ke database staging
def get_staging_engine():
    return create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/staging_adventureworks")

# Engine untuk koneksi ke data warehouse
def get_warehouse_engine():
    return create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/dw_adventureworks")
