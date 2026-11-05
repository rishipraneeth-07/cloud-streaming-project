import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR))

from src.common.pipeline_tracker import track_pipeline_run
from src.tiering.engine import preview_tiering


def main():
    with track_pipeline_run(
        "cloud_storage_tiering",
        "TIERING",
    ):
        preview_tiering()


if __name__ == "__main__":
    main()
