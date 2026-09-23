# Task 9.5

### 1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?

Parquet was the smallest at 3.53 MB, compared to CSV's 8.16 MB and JSONL's 15.69 MB. This happens because Parquet is a columnar format. Storing all values of a single column (like all dates or all prices) next to each other in memory allows for highly efficient dictionary encoding and Snappy compression. Text-based, row-oriented formats like CSV and JSON must repeat column names and store data as inefficient raw strings.

### 2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?

Parquet was the fastest for a full dataset read (median 0.0357s). However, this does not mean it is best for every workload. Parquet is highly optimized for analytical (OLAP) workloads where you read massive amounts of data at once. It is terrible for transactional (OLTP) workloads that require rapid, single-row inserts and updates. PostgreSQL is far better suited for continuous transaction processing.

### 3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?

Parquet was significantly faster for filtered reads (~0.0297s vs PostgreSQL's ~0.0667s). Parquet utilizes "predicate pushdown," meaning it uses metadata headers to completely skip reading chunks of the file that don't contain 'DELIVERED' statuses. PostgreSQL, in its current state, had to perform a full table scan. Adding a B-Tree index on the status column in PostgreSQL would reverse this, allowing the database engine to jump instantly to the 'DELIVERED' rows without scanning the whole table.

### 4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?

In JSON Lines, every line is a complete, independent JSON object. This allows a pipeline to stream and process a file row-by-row without loading the entire 15+ MB file into memory. It also makes appending easy, you just write a new line to the end of the file. A giant JSON array requires parsing the entire document structure into RAM, inserting the new object, and rewriting the closing bracket.

### 5. What happens if a partition key has extremely high cardinality or poor query locality?

If a partition key has high cardinality, it creates the small file problem. You end up with thousands or millions of tiny folders and files. This causes massive metadata overhead for the file system and destroys I/O performance. Poor query locality means users rarely filter by your partition key, forcing the query engine to open and scan hundreds of separate files anyway, which is much slower than just scanning one large file.