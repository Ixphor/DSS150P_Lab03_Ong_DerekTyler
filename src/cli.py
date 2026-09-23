import argparse
import os
import uuid
from pathlib import Path

from src.extract.extractor import extract_sources
from src.transform.transformer import transform_data
from src.load.loader import load_data, load_partition


def resolve_run_id(cli_run_id: str | None) -> str:
    """
    Priority:
      1. --run-id flag (local runs, manual override)
      2. PIPELINE_RUN_ID env var (set by Airflow DAG)
      3. random generated id (purely local ad-hoc runs)
    """
    if cli_run_id:
        return cli_run_id
    env_run_id = os.getenv("PIPELINE_RUN_ID")
    if env_run_id:
        return env_run_id
    return f"run_{uuid.uuid4().hex[:12]}"


def raw_dir_for(run_id: str) -> Path:
    return Path("data/raw") / f"run_id={run_id}"


def curated_dir_for(run_id: str) -> Path:
    return Path("data/curated") / f"run_id={run_id}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    parser.add_argument("--run-id", type=str, help="Global Run ID (overrides PIPELINE_RUN_ID)")
    args, _unknown = parser.parse_known_args()

    run_id = resolve_run_id(args.run_id)

    if args.command == "validate":
        print(f"Validation successful for pipeline run: {run_id}")

    elif args.command == "extract":
        raw_dir = extract_sources(run_id)
        print(f"[extract] run_id={run_id} raw_dir={raw_dir}")

    elif args.command == "transform":
        raw_dir = raw_dir_for(run_id)
        if not raw_dir.is_dir():
            raise FileNotFoundError(
                f"Raw snapshot for run_id={run_id} not found at {raw_dir}. "
                f"Did extract succeed for this same run_id?"
            )
        curated_dir = transform_data(raw_dir, run_id)
        print(f"[transform] run_id={run_id} curated_dir={curated_dir}")

    elif args.command == "load":
        curated_dir = curated_dir_for(run_id)
        if not (curated_dir / "fact_orders.parquet").exists():
            raise FileNotFoundError(
                f"Curated output for run_id={run_id} missing in {curated_dir}. "
                f"Did transform succeed for this same run_id?"
            )
        load_data(curated_dir, run_id)
        print(f"[load] run_id={run_id} loaded from {curated_dir}")

    elif args.command == "load-partition":
        if args.year is None or args.month is None:
            raise ValueError("--year and --month are required for load-partition")
        load_partition(args.year, args.month, run_id)
        print(f"[load-partition] year={args.year} month={args.month} run_id={run_id}")

    elif args.command == "run-all":
        raw_dir = extract_sources(run_id)
        curated_dir = transform_data(raw_dir, run_id)
        load_data(curated_dir, run_id)
        print(f"[run-all] run_id={run_id} complete")

    else:
        raise NotImplementedError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()