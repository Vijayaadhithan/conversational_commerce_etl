"""Select an unchanged retained-ID sample from a verified DuckDB Parquet catalog."""

import gzip
import hashlib
import json
import re
from pathlib import Path

import duckdb

from commerce_etl.pipeline import digest, publication_lock
from commerce_etl.storage import PARQUET_VERSION


def fingerprint(path):
    return {"bytes": path.stat().st_size, "sha256": digest(path)}


def read_source(output):
    pointer = json.loads((output / "current.json").read_text())
    path = (output / pointer["manifest"]).resolve()
    if not path.is_relative_to(output):
        raise ValueError("Source manifest escapes the output directory")
    manifest = json.loads(path.read_text())
    if pointer["run_id"] != manifest["run_id"]:
        raise ValueError("Source pointer and manifest disagree")
    if (
        manifest.get("pipeline_version") != "2.0.0"
        or manifest.get("parquet_version") != PARQUET_VERSION
    ):
        raise ValueError("Retained subsets require the current DuckDB/Parquet contract")
    schema = Path(__file__).resolve().parents[2] / "schema/product-v1.schema.json"
    if digest(schema) != manifest["schema_sha256"]:
        raise ValueError("Source schema differs from the local schema")
    catalog = path.parent / "catalog.parquet"
    if catalog.is_symlink() or fingerprint(catalog) != manifest["outputs"]["catalog.parquet"]:
        raise ValueError("Source Parquet checksum mismatch")
    return path, manifest


def read_reference(path, source):
    meta = json.loads(path.read_text())
    name = meta["fingerprints_file"]
    if meta.get("version") != 1 or Path(name).name != name:
        raise ValueError("Unsupported retained-ID reference")
    fingerprints = path.parent / name
    if fingerprints.is_symlink() or digest(fingerprints) != meta["fingerprints_sha256"]:
        raise ValueError("Retained-ID checksum mismatch")

    def sources(rows):
        return {item["name"]: item["sha256"] for item in rows}

    if sources(meta["sources"]) != sources(source["sources"]):
        raise ValueError("Retained-ID reference belongs to different source archives")
    expected = {}
    with gzip.open(fingerprints, "rt") as handle:
        for line in handle:
            item = json.loads(line)
            key, sha = item["product_id"], item["line_sha256"]
            if key in expected or not re.fullmatch(r"amazon:[A-Z0-9]{10}", key):
                raise ValueError("Duplicate or invalid retained product ID")
            if not re.fullmatch(r"[0-9a-f]{64}", sha):
                raise ValueError("Invalid retained document checksum")
            expected[key] = sha
    if not expected or len(expected) != meta["products"]:
        raise ValueError("Retained-ID count mismatch")
    return meta, expected


def select_retained(source_dir, reference_manifest, output_dir=None):
    """With no output directory, verify selection only and never publish files."""
    source = Path(source_dir).resolve()
    reference = Path(reference_manifest).resolve()
    output = Path(output_dir).resolve() if output_dir is not None else None
    if output is not None and (
        output.is_relative_to(source)
        or source.is_relative_to(output)
        or reference.is_relative_to(output)
    ):
        raise ValueError("Output must be separate from the source and retained-ID reference")
    source_path, manifest = read_source(source)
    reference_sha = digest(reference)
    meta, expected = read_reference(reference, manifest)
    signature = {
        "method": "retained-ids-v1",
        "source_etl_run": manifest["run_id"],
        "source_manifest_sha256": digest(source_path),
        "reference_manifest_sha256": reference_sha,
        "reference_subset_sha256": meta["reference_subset_sha256"],
        "products": len(expected),
        "selector_sha256": digest(Path(__file__)),
    }
    run_id = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:20]
    with duckdb.connect(config={"memory_limit": "2GB", "threads": 1}) as db:
        db.execute("CREATE TABLE wanted(product_id VARCHAR PRIMARY KEY)")
        db.execute("INSERT INTO wanted SELECT unnest(?::VARCHAR[])", [list(expected)])
        db.execute(
            "CREATE TABLE selected AS SELECT p.* FROM read_parquet(?) p JOIN wanted w USING(product_id)",
            [str(source_path.parent / "catalog.parquet")],
        )
        count, unique, eligible = db.execute(
            "SELECT count(*),count(DISTINCT product_id),bool_and(eligible_for_subset) FROM selected"
        ).fetchone()
        if (count, unique, eligible) != (len(expected), len(expected), True):
            raise ValueError("Retained products are missing, duplicated, or ineligible")
        cursor = db.execute("SELECT product_id,document_json FROM selected ORDER BY product_id")
        while rows := cursor.fetchmany(128):
            for key, payload in rows:
                if hashlib.sha256((payload + "\n").encode()).hexdigest() != expected[key]:
                    raise ValueError("Retained product document changed")
        report = {"products": count, "missing_ids": 0, "changed_documents": 0, "published": False}
        if (
            digest(reference) != reference_sha
            or digest(source_path) != signature["source_manifest_sha256"]
            or fingerprint(source_path.parent / "catalog.parquet")
            != manifest["outputs"]["catalog.parquet"]
        ):
            raise ValueError("Source or retained reference changed during selection")
        if output is None:
            return report
        with publication_lock(output):
            run = output / "runs" / run_id
            if run.exists():
                existing = json.loads((run / "manifest.json").read_text())
                if existing.get("sampling") != signature or set(existing["outputs"]) != {
                    "subset.jsonl",
                    "subset.parquet",
                }:
                    raise ValueError("Existing retained subset has a different contract")
                for name, info in existing["outputs"].items():
                    if (run / name).is_symlink() or fingerprint(run / name) != info:
                        raise ValueError("Existing retained subset is corrupt")
            else:
                staging = output / "runs" / (run_id + ".building")
                staging.mkdir(parents=True, exist_ok=False)
                db.execute(
                    "COPY (SELECT * FROM selected ORDER BY product_id) TO ? "
                    "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)",
                    [str(staging / "subset.parquet")],
                )
                cursor = db.execute("SELECT document_json FROM selected ORDER BY product_id")
                with (staging / "subset.jsonl").open("wb") as handle:
                    while rows := cursor.fetchmany(128):
                        for (payload,) in rows:
                            handle.write((payload + "\n").encode())
                derived = {
                    "schema_version": manifest["schema_version"],
                    "schema_sha256": manifest["schema_sha256"],
                    "run_id": run_id,
                    "sources": manifest["sources"],
                    "subset_size": count,
                    "counts": {"subset_rows": count},
                    "sampling": signature,
                    "outputs": {
                        name: fingerprint(staging / name)
                        for name in ("subset.jsonl", "subset.parquet")
                    },
                }
                (staging / "manifest.json").write_text(json.dumps(derived, indent=2) + "\n")
                staging.rename(run)
            # Recheck pinned metadata before changing the output pointer.
            if (
                digest(reference) != reference_sha
                or digest(source_path) != signature["source_manifest_sha256"]
            ):
                raise ValueError("Source or retained reference changed during selection")
            pointer = output / "current.json.tmp"
            pointer.write_text(
                json.dumps({"run_id": run_id, "manifest": f"runs/{run_id}/manifest.json"}, indent=2)
                + "\n"
            )
            pointer.replace(output / "current.json")
            return {
                **report,
                "published": True,
                "run_id": run_id,
                "manifest": str(run / "manifest.json"),
            }
