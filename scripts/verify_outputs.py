"""Independently verify a published catalog and schema-check every subset product."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv
import jsonschema

from commerce_etl import SCHEMA_VERSION
from commerce_etl.pipeline import verify_run


def main():
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser(description="Verify checksums, the complete catalog, and the subset contract")
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", "data/processed"))
    args = parser.parse_args()
    output = Path(args.output_dir).expanduser()
    if not output.is_absolute():
        output = ROOT / output
    output = output.resolve()
    pointer = json.loads((output / "current.json").read_text())
    manifest_path = (output / pointer["manifest"]).resolve()
    if not manifest_path.is_relative_to(output):
        raise ValueError("Manifest path escapes output directory")
    directory = manifest_path.parent
    manifest = json.loads(manifest_path.read_text())
    if pointer["run_id"] != manifest["run_id"]:
        raise ValueError("Run pointer and manifest disagree")
    if any(Path(name).name != name for name in manifest["outputs"]):
        raise ValueError("Output filename must be a basename")
    verify_run(directory)
    schema_path = ROOT / "schema/product-v1.schema.json"
    if hashlib.sha256(schema_path.read_bytes()).hexdigest() != manifest["schema_sha256"]:
        raise ValueError("Local schema differs from the published schema")
    validator = jsonschema.Draft202012Validator(json.loads(schema_path.read_text()))
    subset = {}
    with (directory / "subset.jsonl").open("rb") as stream:
        for line in stream:
            product = json.loads(line)
            validator.validate(product)
            if not product["quality"]["eligible_for_subset"]:
                raise ValueError("Subset contains an ineligible product")
            product_id = product["product_id"]
            if product_id in subset:
                raise ValueError("Subset contains a duplicate product ID")
            subset[product_id] = hashlib.sha256(line).hexdigest()
    rows, observations, matched = 0, 0, 0
    previous = ""
    with gzip.open(directory / "catalog.jsonl.gz", "rb") as stream:
        for line in stream:
            product = json.loads(line)
            product_id = product["product_id"]
            if product_id <= previous or product["schema_version"] != SCHEMA_VERSION:
                raise ValueError("Catalog ordering, uniqueness, or schema version is invalid")
            previous = product_id
            if sum(item["selected"] for item in product["observations"]) != 1:
                raise ValueError("Product does not have exactly one selected observation")
            rows += 1
            observations += len(product["observations"])
            if product_id in subset:
                if subset[product_id] != hashlib.sha256(line).hexdigest():
                    raise ValueError("Subset product differs from the canonical catalog")
                matched += 1
            if rows % 500000 == 0:
                print(f"Verified {rows:,} catalog products", flush=True)
    expected = manifest["counts"]
    if (rows, observations, matched, len(subset)) != (expected["unique_products"], expected["accepted_rows"], expected["subset_rows"], expected["subset_rows"]):
        raise ValueError("Output counts do not agree with manifest")
    print(json.dumps({"passed": True, "run_id": manifest["run_id"], "catalog_rows": rows,
                      "source_observations": observations, "schema_validated_subset_rows": len(subset)}, indent=2), flush=True)


if __name__ == "__main__":
    main()
