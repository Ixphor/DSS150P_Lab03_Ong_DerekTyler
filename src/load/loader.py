import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

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