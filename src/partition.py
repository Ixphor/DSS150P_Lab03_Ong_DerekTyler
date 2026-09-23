import pandas as pd
from pathlib import Path

def get_latest_dir(base_path: str) -> Path:
    base = Path(base_path)
    dirs = sorted([d for d in base.iterdir() if d.is_dir() and "run_id=" in d.name])
    return dirs[-1]

def partition_data():
    curated_dir = get_latest_dir("data/curated")
    df = pd.read_parquet(curated_dir / "fact_orders.parquet")
    
    df['order_year'] = df['source_updated_at'].dt.year
    df['order_month'] = df['source_updated_at'].dt.month
    
    partition_dir = Path("data/partitioned")
    
    df.to_parquet(
        partition_dir,
        engine='pyarrow',
        compression='snappy',
        partition_cols=['order_year', 'order_month'],
        index=False
    )
    print(f"Successfully wrote partitioned data to {partition_dir}")

if __name__ == "__main__":
    partition_data()