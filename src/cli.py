import argparse
import uuid
from pathlib import Path
from src.extract.extractor import extract_sources
from src.transform.transformer import transform_data
from src.load.loader import load_data, load_partition

def get_latest_dir(base_path: str) -> Path:
    base = Path(base_path)
    if not base.exists():
        raise FileNotFoundError(f"Directory {base_path} does not exist.")
    dirs = sorted([d for d in base.iterdir() if d.is_dir() and "run_id=" in d.name])
    if not dirs:
        raise FileNotFoundError(f"No run_id directories found in {base_path}.")
    return dirs[-1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command")
    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)
    args, unknown = parser.parse_known_args()

    run_id = f"run_{uuid.uuid4().hex[:12]}"

    if args.command == "validate-env":
        pass
    elif args.command == "extract":
        extract_sources(run_id)
    elif args.command == "transform":
        raw_dir = get_latest_dir("data/raw")
        transform_data(raw_dir, run_id)
    elif args.command == "load":
        curated_dir = get_latest_dir("data/curated")
        loaded_run_id = curated_dir.name.split("=")[1]
        load_data(curated_dir, loaded_run_id)
    elif args.command == "load-partition":
        if not args.year or not args.month:
            raise ValueError("--year and --month are required for load-partition")
        load_partition(args.year, args.month, run_id)
    elif args.command == "run-all":
        raw_dir = extract_sources(run_id)
        curated_dir = transform_data(raw_dir, run_id)
        load_data(curated_dir, run_id)
    else:
        raise NotImplementedError(f"Unknown command: {args.command}")

if __name__ == "__main__":
    main()