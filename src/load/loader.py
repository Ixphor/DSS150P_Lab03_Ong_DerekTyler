import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def load_data(curated_dir: Path, run_id: str):
    try:
        fact_path = curated_dir / "fact_orders.parquet"
        if not fact_path.exists():
            return
            
        df = pd.read_parquet(fact_path)
        if df.empty:
            return

        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'dss150p'),
            user=os.getenv('POSTGRES_USER', 'dss150p'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        conn.autocommit = True
        cursor = conn.cursor()

        columns = df.columns.tolist()
        values = [tuple(x) for x in df.to_numpy()]

        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'order_id'])
        
        insert_query = f"""
            INSERT INTO curated.sales_order_lines ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                {update_set}
            WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
        """

        execute_values(cursor, insert_query, values)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Load stage failed for run_id {run_id}: {str(e)}")
        raise RuntimeError(f"Load stage failed: {str(e)}") from e

def load_partition(year: int, month: int, run_id: str):
    try:
        print(f"-> Attempting to load partition for year={year}, month={month}...")
        
        base_path = Path("data/partitioned")
        if not base_path.exists():
            print(f"-> ERROR: {base_path} does not exist. Did you run the partition script?")
            return
            
        year_dirs = [d for d in base_path.iterdir() if d.is_dir() and str(year) in d.name]
        if not year_dirs:
            print(f"-> ERROR: Could not find a folder for year {year} in {base_path}")
            return
            
        year_dir = year_dirs[0]
        month_dirs = [d for d in year_dir.iterdir() if d.is_dir() and str(month) in d.name]
        if not month_dirs:
            print(f"-> ERROR: Could not find a folder for month {month} in {year_dir}")
            return
            
        partition_path = month_dirs[0]
        print(f"-> Successfully found partition folder: {partition_path}")
        
        df = pd.read_parquet(partition_path)
        if df.empty:
            print("-> ERROR: Found the folder, but the partition is empty.")
            return

        if 'order_year' in df.columns:
            df = df.drop(columns=['order_year'])
        if 'order_month' in df.columns:
            df = df.drop(columns=['order_month'])

        conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB', 'dss150p'),
            user=os.getenv('POSTGRES_USER', 'dss150p'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
        conn.autocommit = True
        cursor = conn.cursor()

        columns = df.columns.tolist()
        values = [tuple(x) for x in df.to_numpy()]

        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'order_id'])
        
        insert_query = f"""
            INSERT INTO curated.sales_order_lines ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                {update_set}
            WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash;
        """

        execute_values(cursor, insert_query, values)
        
        partition_key = f"{year}-{month}"
        row_count = len(df)
        now_utc = datetime.now(timezone.utc)
        
        # The UPSERT logic added here prevents the Primary Key crash
        cursor.execute("""
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (partition_key) DO UPDATE SET
                loaded_at_utc = EXCLUDED.loaded_at_utc,
                row_count = EXCLUDED.row_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """, (partition_key, now_utc, row_count, run_id))

        cursor.close()
        conn.close()
        print(f"-> SUCCESS: Partition {partition_key} ({row_count} rows) loaded and audited!")
        
    except Exception as e:
        logger.error(f"Partition load failed for {year}-{month}: {str(e)}")
        raise RuntimeError(f"Partition load failed: {str(e)}") from e