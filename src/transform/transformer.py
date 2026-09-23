import json
import hashlib
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

def transform_data(raw_dir: Path, run_id: str) -> Path:
    try:
        curated_dir = Path(f"data/curated/run_id={run_id}")
        quarantine_dir = Path(f"data/quarantine/run_id={run_id}")
        curated_dir.mkdir(parents=True, exist_ok=True)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        
        customers = pd.read_csv(raw_dir / "customers.csv")
        orders = pd.read_csv(raw_dir / "orders.csv")
        with open(raw_dir / "products.json", 'r') as f:
            products_raw = json.load(f)
        products = pd.json_normalize(products_raw)
        
        customers.columns = customers.columns.str.strip().str.lower()
        orders.columns = orders.columns.str.strip().str.lower()
        products.columns = products.columns.str.strip().str.lower()
        
        now_utc = pd.Timestamp.now(tz='UTC')

        customers['updated_at'] = pd.to_datetime(customers['updated_at'], utc=True)
        customers = customers.sort_values('updated_at').groupby('customer_id').tail(1)
        customers['email'] = customers['email'].astype(str).str.strip().str.lower().replace({'nan': pd.NA, 'none': pd.NA, '': pd.NA})
        if 'city' in customers.columns:
            customers['city'] = customers['city'].astype(str).str.strip().str.title().replace({'nan': pd.NA, 'none': pd.NA, '': pd.NA})

        products['updated_at'] = pd.to_datetime(products['updated_at'], utc=True)
        products = products.sort_values('updated_at').groupby('product_id').tail(1)
        products['unit_price'] = pd.to_numeric(products['unit_price'], errors='coerce')
        valid_price = products['unit_price'].notna() & (products['unit_price'] >= 0)
        products_valid = products[valid_price].copy()
        products_quarantine = products[~valid_price].copy()

        orders['updated_at'] = pd.to_datetime(orders['updated_at'], utc=True)
        if 'order_timestamp' in orders.columns:
            orders['order_timestamp'] = pd.to_datetime(orders['order_timestamp'], utc=True)
        orders = orders.sort_values('updated_at').groupby('order_id').tail(1)
        orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')
        valid_order = (
            orders['quantity'].notna() & 
            (orders['quantity'] >= 1) & 
            (orders['quantity'] <= 20) &
            orders['status'].astype(str).str.lower().isin([
                'pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned'
            ])
        )
        orders_valid = orders[valid_order].copy()
        orders_quarantine = orders[~valid_order].copy()

        products_merge = products_valid[['product_id', 'unit_price']].rename(columns={'unit_price': 'prod_price'})

        joined = orders_valid.merge(
            customers[['customer_id']], on='customer_id', how='left', indicator='_merge_cust'
        ).merge(
            products_merge, on='product_id', how='left', indicator='_merge_prod'
        )
        
        is_orphan = (joined['_merge_cust'] == 'left_only') | (joined['_merge_prod'] == 'left_only')
        
        orphans = joined[is_orphan].copy()
        if not orphans.empty:
            orphans['quarantine_reason'] = "Orphan reference: missing valid customer or product"
            orphans = orphans.drop(columns=['_merge_cust', '_merge_prod', 'prod_price'], errors='ignore')
            orphans['pipeline_run_id'] = run_id
            orphans['processed_at_utc'] = now_utc
            orphans.to_parquet(quarantine_dir / "orphan_orders.parquet", index=False)
            
        curated_orders = joined[~is_orphan].copy()
        if not curated_orders.empty:
            curated_orders = curated_orders.drop(columns=['_merge_cust', '_merge_prod'])
            
            if 'unit_price' not in curated_orders.columns:
                curated_orders = curated_orders.rename(columns={'prod_price': 'unit_price'})
            else:
                curated_orders = curated_orders.drop(columns=['prod_price'])
            
            curated_orders['gross_amount'] = curated_orders['quantity'] * curated_orders['unit_price']
            if 'discount_pct' not in curated_orders.columns:
                curated_orders['discount_pct'] = 0.0
            curated_orders['discount_pct'] = pd.to_numeric(curated_orders['discount_pct'], errors='coerce').fillna(0.0)
            curated_orders['discount_amount'] = curated_orders['gross_amount'] * curated_orders['discount_pct']
            curated_orders['net_amount'] = curated_orders['gross_amount'] - curated_orders['discount_amount']
            
            curated_orders['source_updated_at'] = curated_orders['updated_at']
            curated_orders = curated_orders.drop(columns=['updated_at'])
            curated_orders['pipeline_run_id'] = run_id
            curated_orders['processed_at_utc'] = now_utc
            
            hash_cols = ['order_id', 'customer_id', 'product_id', 'quantity', 'status', 'gross_amount', 'net_amount']
            hash_data = curated_orders[hash_cols].astype(str).fillna('')
            curated_orders['record_hash'] = hash_data.apply(
                lambda row: hashlib.sha256("".join(row).encode('utf-8')).hexdigest(), axis=1
            )
            
            curated_orders.to_parquet(curated_dir / "fact_orders.parquet", index=False)

        dfs = [
            (customers, curated_dir / "customers.parquet"),
            (products_valid, curated_dir / "products.parquet"),
            (products_quarantine, quarantine_dir / "products_quarantined.parquet"),
            (orders_valid, curated_dir / "orders_staged.parquet"),
            (orders_quarantine, quarantine_dir / "orders_quarantined.parquet")
        ]
        
        for df, path in dfs:
            if not df.empty:
                df = df.copy()
                df['pipeline_run_id'] = run_id
                df['staged_at_utc'] = now_utc
                df.to_parquet(path, index=False)
                
        return curated_dir
    except Exception as e:
        logger.error(f"Transformation stage failed for run_id {run_id}: {str(e)}")
        raise RuntimeError(f"Transformation stage failed: {str(e)}") from e