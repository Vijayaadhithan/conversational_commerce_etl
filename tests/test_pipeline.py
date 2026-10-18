import gzip
import json
from pathlib import Path
import subprocess
import sys

import jsonschema
import pytest

from commerce_etl.pipeline import digest, quotas_for, run_pipeline

ROOT = Path(__file__).resolve().parent.parent


def record(asin="B000000001", **kwargs):
    return {"parent_asin": asin, "title": "Fragrance-free face moisturizer", "price": "12.99",
            "categories": ["Beauty & Personal Care", "Skin Care"], "features": ["For dry skin"],
            "details": {"Brand": "Example", "Skin Type": "Dry"}, **kwargs}


def inputs(tmp_path, rows_by_source):
    raw = tmp_path / "raw"
    raw.mkdir()
    sources = []
    for source, rows in rows_by_source.items():
        filename = source + ".jsonl.gz"
        with gzip.open(raw / filename, "wt") as stream:
            for item in rows:
                stream.write((item if isinstance(item, str) else json.dumps(item)) + "\n")
        sources.append({"name": source, "filename": filename, "sha256": digest(raw / filename), "expected_rows": len(rows)})
    manifest = tmp_path / "sources.json"
    manifest.write_text(json.dumps({"currency": "USD", "sources": sources}))
    return raw, manifest


def read_catalog(directory):
    with gzip.open(directory / "catalog.jsonl.gz", "rt") as stream:
        return [json.loads(line) for line in stream]


def test_complete_exports_schema_provenance_and_conflict_exclusion(tmp_path):
    raw, manifest = inputs(tmp_path, {
        "All_Beauty": [record(), record("B000000002"), "not json", {"title": "No ID"}],
        "Beauty_and_Personal_Care": [record(price=15, details={"Brand": "Example", "Skin Type": "Dry", "Size": "Large"}),
                                     record("B000000003", price=None)],
    })
    directory, result = run_pipeline(raw, tmp_path / "out", manifest, subset_size=20)
    products = read_catalog(directory)
    assert result["counts"] == {"input_rows": 6, "accepted_rows": 4, "rejected_rows": 2, "unique_products": 3, "subset_rows": 1}
    schema = json.loads((ROOT / "schema/product-v1.schema.json").read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    for product in products:
        jsonschema.validate(product, schema)
    first = products[0]
    assert first["price"]["amount"] == "15.00"
    assert first["attributes"]["size"]["value"] == "Large"
    assert first["selected_observation"]["source"] == "Beauty_and_Personal_Care"
    assert len(first["observations"]) == 2
    assert sum(item["selected"] for item in first["observations"]) == 1
    assert "price" in first["quality"]["conflicting_fields"]
    assert first["quality"]["eligible_for_subset"] is False
    subset = [json.loads(line) for line in (directory / "subset.jsonl").read_text().splitlines()]
    assert subset[0]["product_id"] == "amazon:B000000002"
    quality = json.loads((directory / "data_quality.json").read_text())
    assert quality["duplicate_observations"] == 1 and quality["subset_shortfall"] == 19
    assert not (directory / "work.sqlite3").exists()


def test_reproducible_gzip_subset_and_safe_reuse(tmp_path):
    raw, manifest = inputs(tmp_path, {"Amazon_Fashion": [
        record(f"B{index:09d}", title="Blue cotton shirt", categories=[], features=["Cotton fabric"])
        for index in range(20)
    ]})
    before = digest(next(raw.iterdir()))
    first, a = run_pipeline(raw, tmp_path / "one", manifest, subset_size=7, seed="fixed")
    second, b = run_pipeline(raw, tmp_path / "two", manifest, subset_size=7, seed="fixed")
    assert a["run_id"] == b["run_id"]
    assert a["outputs"] == b["outputs"]
    assert digest(next(raw.iterdir())) == before
    reused, _ = run_pipeline(raw, tmp_path / "one", manifest, subset_size=7, seed="fixed")
    assert reused == first
    (first / "subset.jsonl").write_text("corrupt")
    pointer = (tmp_path / "one/current.json").read_bytes()
    with pytest.raises(ValueError, match="integrity"):
        run_pipeline(raw, tmp_path / "one", manifest, subset_size=7, seed="fixed")
    assert (tmp_path / "one/current.json").read_bytes() == pointer


def test_manifest_order_does_not_change_output(tmp_path):
    raw, manifest = inputs(tmp_path, {"All_Beauty": [record()], "Beauty_and_Personal_Care": [record(price=15)]})
    _, first = run_pipeline(raw, tmp_path / "one", manifest, 2)
    config = json.loads(manifest.read_text())
    config["sources"].reverse()
    manifest.write_text(json.dumps(config))
    _, second = run_pipeline(raw, tmp_path / "two", manifest, 2)
    assert first["run_id"] == second["run_id"]
    assert first["outputs"] == second["outputs"]


def test_checksums_and_input_output_separation(tmp_path):
    raw, manifest = inputs(tmp_path, {"All_Beauty": [record()]})
    with pytest.raises(ValueError, match="separate"):
        run_pipeline(raw, raw / "out", manifest)
    with (raw / "All_Beauty.jsonl.gz").open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        run_pipeline(raw, tmp_path / "out", manifest)
    assert not (tmp_path / "out").exists()


def test_quarantine_does_not_publish_empty_catalog_or_replace_current(tmp_path):
    raw, manifest = inputs(tmp_path, {"All_Beauty": [{"title": "missing ID"}, "NaN"]})
    output = tmp_path / "out"
    output.mkdir()
    (output / "current.json").write_text('{"existing":"keep"}')
    before = (output / "current.json").read_bytes()
    with pytest.raises(ValueError, match="No valid"):
        run_pipeline(raw, output, manifest)
    assert (output / "current.json").read_bytes() == before
    assert len(list((output / "runs").glob("*.building"))) == 1


def test_sampling_redistributes_small_strata_without_exceeding_capacity():
    result = quotas_for({("a",): 1, ("b",): 100, ("c",): 100}, 20)
    assert result[("a",)] == 1
    assert sum(result.values()) == 20
    assert abs(result[("b",)] - result[("c",)]) <= 1
    assert quotas_for({}, 20) == {}


def test_independent_verifier_rejects_subset_drift_even_with_updated_file_checksum(tmp_path):
    raw, manifest = inputs(tmp_path, {"All_Beauty": [record()]})
    output = tmp_path / "out"
    directory, _ = run_pipeline(raw, output, manifest, 1)
    command = [sys.executable, str(ROOT / "scripts/verify_outputs.py"), "--output-dir", str(output)]
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    product = json.loads((directory / "subset.jsonl").read_text())
    product["price"]["amount"] = "999.00"
    subset = directory / "subset.jsonl"
    subset.write_text(json.dumps(product) + "\n")
    published = json.loads((directory / "manifest.json").read_text())
    published["outputs"]["subset.jsonl"] = {"bytes": subset.stat().st_size, "sha256": digest(subset)}
    (directory / "manifest.json").write_text(json.dumps(published))
    result = subprocess.run(command, capture_output=True, text=True)
    assert result.returncode != 0
    assert "differs from the canonical" in result.stderr
