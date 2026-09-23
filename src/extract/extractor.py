import shutil
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def extract_sources(run_id: str) -> Path:
    try:
        source_dir = Path("data/source")
        raw_dir = Path(f"data/raw/run_id={run_id}")
        
        raw_dir.mkdir(parents=True, exist_ok=True)
        
        source_files = ["customers.csv", "products.json", "orders.csv"]
        
        for file_name in source_files:
            source_file = source_dir / file_name
            target_file = raw_dir / file_name
            
            if source_file.is_file():
                shutil.copy2(source_file, target_file)
                
        return raw_dir
    except Exception as e:
        logger.error(f"Extraction stage failed for run_id {run_id}: {str(e)}")
        raise RuntimeError(f"Extraction stage failed: {str(e)}") from e