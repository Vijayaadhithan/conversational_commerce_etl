import argparse
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from commerce_etl.pipeline import run_pipeline

ROOT = Path(__file__).resolve().parents[2]


def main():
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(
        description="Audit and normalize historical Amazon fashion/beauty metadata"
    )
    parser.add_argument(
        "--raw-dir",
        default=os.getenv("RAW_DATA_DIR", "../conversational_commerce/data/public_sources/raw"),
    )
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", "data/processed"))
    parser.add_argument("--sources", default="sources.json")
    parser.add_argument("--subset-size", type=int, default=int(os.getenv("SUBSET_SIZE", "20000")))
    parser.add_argument("--seed", default=os.getenv("SUBSET_SEED", "commerce-v1"))
    parser.add_argument(
        "--keep-work-db",
        action="store_true",
        help="Retain the local DuckDB work file for inspection",
    )
    parser.add_argument("--memory-limit", default=os.getenv("DUCKDB_MEMORY_LIMIT", "2GB"))
    parser.add_argument("--threads", type=int, default=int(os.getenv("DUCKDB_THREADS", "1")))
    args = parser.parse_args()

    def resolve(value):
        path = Path(value).expanduser()
        return path if path.is_absolute() else ROOT / path

    try:
        directory, manifest = run_pipeline(
            resolve(args.raw_dir),
            resolve(args.output_dir),
            resolve(args.sources),
            args.subset_size,
            args.seed,
            args.keep_work_db,
            args.memory_limit,
            args.threads,
        )
    except (OSError, ValueError, KeyError, duckdb.Error) as exc:
        parser.exit(
            1, f"ETL failed: {exc}\nThe previously published catalog pointer remains unchanged.\n"
        )
    print(f"Output: {directory}", flush=True)
    print(
        f"Unique products: {manifest['counts']['unique_products']:,}; subset: {manifest['counts']['subset_rows']:,}",
        flush=True,
    )


if __name__ == "__main__":
    main()
