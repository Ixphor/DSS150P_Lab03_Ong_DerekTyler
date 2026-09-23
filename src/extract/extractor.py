import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

REQUIRED_SOURCE_FILES = ["customers.csv", "products.json", "orders.csv"]


def extract_sources(run_id: str) -> Path:
    try:
        source_dir = Path("data/source")
        raw_dir = Path(f"data/raw/run_id={run_id}")
        raw_dir.mkdir(parents=True, exist_ok=True)

        missing = []
        for file_name in REQUIRED_SOURCE_FILES:
            source_file = source_dir / file_name
            if not source_file.is_file():
                missing.append(str(source_file))
                continue
            shutil.copy2(source_file, raw_dir / file_name)

        if missing:
            raise FileNotFoundError(
                f"Missing required source file(s): {', '.join(missing)}"
            )

        logger.info("Extracted %d files into %s", len(REQUIRED_SOURCE_FILES), raw_dir)
        return raw_dir

    except FileNotFoundError:
        # re-raise so Airflow shows the real reason
        logger.error("Extraction failed for run_id %s: required source file missing", run_id)
        raise
    except Exception as e:
        logger.error(f"Extraction stage failed for run_id {run_id}: {str(e)}")
        raise RuntimeError(f"Extraction stage failed: {str(e)}") from e