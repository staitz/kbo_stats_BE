import sys
import os
from pathlib import Path

# Setup Django environment to load settings
ROOT = Path(__file__).resolve().parents[1]
DJANGO_ROOT = ROOT / "be"
SQLITE_DB = ROOT / "kbo_stats.db"

sys.path.insert(0, str(DJANGO_ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django
django.setup()
from django.conf import settings

import pandas as pd
from sqlalchemy import create_engine
import sqlite3

def main():
    db = settings.DATABASES["default"]
    
    # Check if we are connected to PostgreSQL
    if db["ENGINE"] != "django.db.backends.postgresql":
        print("Django backend is not currently pointing to PostgreSQL (Check USE_POSTGRES in .env).")
        print("Cannot migrate data if not connected to PostgreSQL first.")
        return

    # Create SQLAlchemy engine for PostgreSQL
    # Using psycopg version 3 which uses postgresql+psycopg
    pg_url = f"postgresql+psycopg://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:{db['PORT']}/{db['NAME']}"
    pg_engine = create_engine(pg_url)

    # SQLite connection
    sqlite_conn = sqlite3.connect(str(SQLITE_DB))

    # Fetch all table names from PostgreSQL (excluding system tables)
    print("Fetching tables from PostgreSQL...")
    tables_query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"
    with pg_engine.connect() as conn:
        tables_df = pd.read_sql(tables_query, conn)
    
    tables = tables_df['tablename'].tolist()
    print(f"Found {len(tables)} tables to migrate.")

    for table in tables:
        print(f"Migrating table: {table} ...", end=" ")
        try:
            with pg_engine.connect() as conn:
                df = pd.read_sql(f"SELECT * FROM {table}", conn)
            
            if len(df) > 0:
                # Write to SQLite
                df.to_sql(table, con=sqlite_conn, if_exists='replace', index=False)
                print(f"[OK] {len(df)} rows")
            else:
                # Still create empty table structure slightly implicitly via Pandas
                df.to_sql(table, con=sqlite_conn, if_exists='replace', index=False)
                print(f"[SKIPPED] 0 rows")
        except Exception as e:
            print(f"[ERROR] {e}")

    sqlite_conn.close()
    print("Migration from PostgreSQL to SQLite completed successfully!")
    print(f"Saved into: {SQLITE_DB}")

if __name__ == "__main__":
    main()
