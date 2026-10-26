"""Verify or publish a retained-ID demo subset from the full Parquet catalog."""

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from commerce_etl.retained_subset import select_retained


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=ROOT / "data/processed")
    parser.add_argument("--reference-manifest", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-only", action="store_true")
    mode.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    result = select_retained(args.source_dir, args.reference_manifest, args.output_dir)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
