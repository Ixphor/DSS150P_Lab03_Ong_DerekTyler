# Technical Questions

1. Why is record_hash useful for rerun-safe loading, and which columns should not be included in it?

record_hash acts as a unique digital fingerprint for the content of a row. When loading data, the database compares the incoming hash to the existing hash. If they match, the pipeline knows the data hasn't changed and skips the update, making the pipeline rerun-safe (idempotent) and saving massive amounts of processing power.

2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

Raw data serves as the immutable "source of truth." If a bug is discovered in your transformation logic, or if the business suddenly requires a new curated metric, you cannot retroactively extract that history from the source system (as the source may have changed or deleted old records). By preserving the raw data in a data lake, you can completely rebuild your curated tables from scratch at any time.

3. What is the difference between a data-quality rejection and a system exception?

Data-quality rejection: This is a business-level issue (e.g., a customer is missing an email address, or an order total is negative). The pipeline should not crash. It should gracefully catch the bad record, write it to a "dead-letter queue" or log, and continue processing the rest of the valid data.

System exception: This is an infrastructural or code failure (e.g., Errno 13 Permission denied, a database connection timeout, or running out of memory). The pipeline should crash or trigger a retry, as the environment itself is compromised and cannot safely process data.

4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Parquet is a columnar storage format, whereas CSV is row-oriented. In analytical workloads, queries usually aggregate or filter specific columns. Parquet allows the query engine to completely ignore the columns it doesn't need, drastically reducing disk I/O. Furthermore, storing similar data types together allows Parquet to use highly efficient dictionary encoding and compression (like Snappy), resulting in much smaller file sizes than plain-text CSVs.

5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

If you write heavy data manipulation code directly inside the Airflow pipeline_dag.py file, you tightly couple your orchestration with your business logic. It makes the DAG file huge, difficult to read, impossible to test locally without spinning up Airflow, and places unnecessary processing strain on the Airflow scheduler. Logic should remain in modular Python files (like your src/ folder), and the DAG should merely trigger them.

6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

Retries depend on idempotency to be safe. Idempotency means executing a task multiple times yields the exact same final state as executing it once. Example, a pipeline that simply runs an INSERT statement to load 1,000 rows. If the network drops after 500 rows, the task fails. Airflow automatically retries the task and inserts all 1,000 rows. You now have 1,500 rows in your database—500 of them are duplicates.

7. What trade-off is introduced by partitioning too aggressively?

Aggressive partitioning creates the "small file problem." Instead of a few efficiently sized files, the system generates thousands or millions of tiny folders and files. This causes massive metadata overhead for the file system and destroys read performance, as the query engine spends more time opening and closing files than actually reading data.

8. How would you adapt the pipeline if the source became an API or database instead of local files?

Because this pipeline is heavily modular, the adaptation is extremely minimal. You would only need to rewrite the src/extract/extractor.py module to connect to the new API (using requests) or database (using SQLAlchemy). As long as the extractor still saves the resulting data into the data/raw/ directory in the expected format (like CSV or Parquet), the transform and load modules wouldn't need to change at all.