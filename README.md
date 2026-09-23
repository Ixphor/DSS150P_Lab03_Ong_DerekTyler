# DSS150P Lab 03

**Derek Tyler U. Ong**  
**2022135297**

## Overview

This repository contains a reproducible data engineering pipeline that extracts raw e-commerce source files, stages and curates them, benchmarks storage formats, partitions the curated dataset by year/month, loads a PostgreSQL warehouse, and orchestrates the whole flow with Apache Airflow.

The pipeline is built around four layers:

| Layer | Purpose | Output |
|---|---|---|
| **Raw** | Reproducible snapshot per run | `data/raw/run_id=<run_id>/` |
| **Staging** | Typing, normalization, deduplication, technical validation | `data/curated/run_id=<run_id>/customers.parquet`, `orders_staged.parquet`, `products.parquet` |
| **Curated** | Cross-source joins and business measures | `data/curated/run_id=<run_id>/fact_orders.parquet` |
| **Quarantine** | Invalid records retained with a reason | `data/quarantine/run_id=<run_id>/` |

## Repository Layout

```text
project/
├── .env.example
├── .gitignore
├── Dockerfile
├── Dockerfile.airflow
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
├── requirements-airflow.txt
├── config/settings.yml
├── data/
│   ├── source/        # immutable source datasets
│   ├── raw/           # run-specific snapshots
│   ├── curated/       # staging + curated outputs
│   ├── quarantine/    # invalid records with reasons
│   ├── benchmark/     # CSV / JSONL / Parquet materializations
│   └── partitioned/   # partitioned Parquet by year/month
├── src/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   ├── validate/
│   ├── benchmark.py
│   ├── partition.py
│   ├── config.py
│   ├── common/
│   └── cli.py
├── dags/
│   └── dss150p_pipeline.py
├── sql/init/
├── templates/
├── docs/
│   ├── answers/
│   └── evidence/
└── tests/
```

## Prerequisites

- Python 3.11 or newer
- Git
- Docker Desktop or Docker Engine with Compose v2
- At least 6 GB free disk space
- Text editor or IDE

## Initial Setup

```bash
git clone your-repo-url
cd DSS150P_Lab03_Ong_DerekTyler
cp .env.example .env
```
*Edit `.env` and set `POSTGRES_PASSWORD` to a non-default value.*

```bash
python -m venv .venv
```

## Configuration

Non-secret defaults live in `config/settings.yml`. Environment-specific values live in `.env`. `src/config.py` is the only place that reads both and turns them into runtime settings. Business logic never reads `.env` or `settings.yml` directly.

## Pipeline Commands

All commands run through the CLI: `python -m src.cli <command>`.

| Command | Purpose |
|---|---|
| `validate-env` | Verify Python, packages, config, env vars, and PostgreSQL connectivity |
| `extract` | Copy source files into a run-specific raw folder |
| `transform` | Build staging, curated, and quarantine outputs |
| `load` | UPSERT curated fact table into PostgreSQL |
| `load-partition --year Y --month M` | Load a single year/month partition |
| `validate` | Assert curated output exists and is well-formed |
| `run-all` | Run extract + transform + load in one invocation |
| `benchmark --repeats N` | Materialize CSV/JSONL/Parquet and time reads |
| `partition` | Write partitioned Parquet by order_year and order_month |

## End-to-End Sequence

**1. Environment check**
```bash
python -m src.cli validate-env
```

**2. Start Postgres**
```bash
docker compose up -d postgres
```

**3. Full local pipeline**
```bash
python -m src.cli run-all
python -m src.cli load
python -m src.cli validate
```

**4. Benchmark**
```bash
python -m src.cli benchmark --repeats 5
```

**5. Partitioned Parquet + selected partition load**
```bash
python -m src.cli partition
python -m src.cli load-partition --year 2026 --month 1
```

**6. Containerized check**
```bash
docker compose build pipeline
docker compose run --rm pipeline python -m src.cli validate-env
```

**7. Airflow**
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
*Open http://localhost:8080 (credentials: admin / admin)*

## Secrets and .gitignore

`.env` is never committed. `.gitignore` excludes `.env`, `.venv/`, `__pycache__/`, `data/.last_run_id`, and runtime logs. Passwords are never hard-coded in Python, YAML, SQL, or DAG files.