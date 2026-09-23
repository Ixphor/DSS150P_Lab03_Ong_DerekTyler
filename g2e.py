import pandas as pd
from pathlib import Path

def get_latest(base_path):
    dirs = sorted([d for d in Path(base_path).iterdir() if d.is_dir() and "run_id=" in d.name])
    return dirs[-1]

raw, cur, quar = get_latest("data/raw"), get_latest("data/curated"), get_latest("data/quarantine")

print("=== 1. ROW COUNTS ===")
print(f"Raw Orders: {len(pd.read_csv(raw / 'orders.csv'))}")
print(f"Staged Orders: {len(pd.read_parquet(cur / 'orders_staged.parquet'))}")
print(f"Curated Fact Orders: {len(pd.read_parquet(cur / 'fact_orders.parquet'))}")
print(f"Quarantined Invalid Orders: {len(pd.read_parquet(quar / 'orders_quarantined.parquet'))}")
print(f"Quarantined Orphans: {len(pd.read_parquet(quar / 'orphan_orders.parquet'))}")

print("\n=== 2. SAMPLE AUDIT COLUMNS ===")
df = pd.read_parquet(cur / 'fact_orders.parquet')
print(df[['pipeline_run_id', 'source_updated_at', 'processed_at_utc', 'record_hash']].head(3).to_string(index=False))