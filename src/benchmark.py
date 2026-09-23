import os
import time
import statistics
import warnings
import pandas as pd
import psycopg2
from pathlib import Path

warnings.filterwarnings('ignore', category=UserWarning)

def get_latest_dir(base_path: str) -> Path:
    base = Path(base_path)
    dirs = sorted([d for d in base.iterdir() if d.is_dir() and "run_id=" in d.name])
    return dirs[-1]

def measure_read(func, *args, **kwargs):
    times = []
    for _ in range(5):
        start = time.perf_counter()
        func(*args, **kwargs)
        times.append(time.perf_counter() - start)
    return statistics.median(times)

def run_benchmark():
    curated_dir = get_latest_dir("data/curated")
    df = pd.read_parquet(curated_dir / "fact_orders.parquet")
    
    bench_dir = Path("data/benchmark")
    bench_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = bench_dir / "orders.csv"
    jsonl_path = bench_dir / "orders.jsonl"
    parquet_path = bench_dir / "orders.parquet"
    out_csv = bench_dir / "benchmark_results.csv"
    
    results = []
    total_rows = len(df)

    start = time.perf_counter()
    df.to_csv(csv_path, index=False)
    write_csv = time.perf_counter() - start
    
    read_csv = measure_read(pd.read_csv, csv_path)
    read_csv_filt = measure_read(lambda: pd.read_csv(csv_path).query("status == 'delivered' | status == 'DELIVERED'"))
    
    results.append({
        "storage_type": "CSV", 
        "file_size_bytes": os.path.getsize(csv_path), 
        "write_seconds": round(write_csv, 4), 
        "full_read_seconds": round(read_csv, 4), 
        "filtered_read_seconds": round(read_csv_filt, 4),
        "row_count": total_rows,
        "notes": "Standard to_csv"
    })
    
    start = time.perf_counter()
    df.to_json(jsonl_path, orient="records", lines=True, date_format="iso")
    write_jsonl = time.perf_counter() - start
    
    read_jsonl = measure_read(pd.read_json, jsonl_path, orient="records", lines=True)
    read_jsonl_filt = measure_read(lambda: pd.read_json(jsonl_path, orient="records", lines=True).query("status == 'delivered' | status == 'DELIVERED'"))
    
    results.append({
        "storage_type": "JSON Lines", 
        "file_size_bytes": os.path.getsize(jsonl_path), 
        "write_seconds": round(write_jsonl, 4), 
        "full_read_seconds": round(read_jsonl, 4), 
        "filtered_read_seconds": round(read_jsonl_filt, 4),
        "row_count": total_rows,
        "notes": "Records lines format"
    })

    start = time.perf_counter()
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    write_parquet = time.perf_counter() - start
    
    read_parquet = measure_read(pd.read_parquet, parquet_path)
    read_parquet_filt = measure_read(pd.read_parquet, parquet_path, filters=[('status', 'in', ['delivered', 'DELIVERED'])]) 
    
    results.append({
        "storage_type": "Parquet", 
        "file_size_bytes": os.path.getsize(parquet_path), 
        "write_seconds": round(write_parquet, 4), 
        "full_read_seconds": round(read_parquet, 4), 
        "filtered_read_seconds": round(read_parquet_filt, 4),
        "row_count": total_rows,
        "notes": "Snappy compression"
    })

    conn = psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'dss150p'),
        user=os.getenv('POSTGRES_USER', 'dss150p'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432')
    )
    
    cur = conn.cursor()
    cur.execute("SELECT pg_total_relation_size('curated.sales_order_lines');")
    size_pg = cur.fetchone()[0]
    cur.close()
    
    read_pg = measure_read(pd.read_sql, "SELECT * FROM curated.sales_order_lines", conn)
    read_pg_filt = measure_read(pd.read_sql, "SELECT * FROM curated.sales_order_lines WHERE UPPER(status) = 'DELIVERED'", conn)
    
    results.append({
        "storage_type": "PostgreSQL", 
        "file_size_bytes": size_pg, 
        "write_seconds": "", 
        "full_read_seconds": round(read_pg, 4), 
        "filtered_read_seconds": round(read_pg_filt, 4),
        "row_count": total_rows,
        "notes": "Pre-loaded table size"
    })
    
    conn.close()

    res_df = pd.DataFrame(results)
    res_df.to_csv(out_csv, index=False)
    print(res_df.to_string(index=False))

if __name__ == "__main__":
    run_benchmark()