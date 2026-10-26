import gzip
import hashlib
import json

import pytest
from commerce_etl.pipeline import digest, run_pipeline
from commerce_etl.retained_subset import select_retained
from test_pipeline import inputs, record


def fixture_catalog(tmp_path):
    raw, sources = inputs(
        tmp_path, {"All_Beauty": [record(), record("B000000002"), record("B000000003")]}
    )
    source = tmp_path / "source"
    run, manifest = run_pipeline(raw, source, sources, 3)
    # Preserve a specific late ID, not the first balanced sample from a fresh run.
    line = (run / "subset.jsonl").read_bytes().splitlines(keepends=True)[-1]
    payload = json.loads(line)
    ids = tmp_path / "ids.jsonl.gz"
    with gzip.open(ids, "wt") as handle:
        handle.write(
            json.dumps(
                {
                    "product_id": payload["product_id"],
                    "line_sha256": hashlib.sha256(line).hexdigest(),
                }
            )
            + "\n"
        )
    reference = tmp_path / "reference.json"
    reference.write_text(
        json.dumps(
            {
                "version": 1,
                "products": 1,
                "sources": manifest["sources"],
                "reference_subset_sha256": hashlib.sha256(line).hexdigest(),
                "fingerprints_file": ids.name,
                "fingerprints_sha256": digest(ids),
            }
        )
    )
    return source, reference, payload


def test_check_only_and_reproducible_retention_without_source_mutation(tmp_path):
    source, reference, product = fixture_catalog(tmp_path)
    pointer = (source / "current.json").read_bytes()
    checked = select_retained(source, reference)
    assert checked == {"products": 1, "missing_ids": 0, "changed_documents": 0, "published": False}
    result = select_retained(source, reference, tmp_path / "one")
    assert select_retained(source, reference, tmp_path / "one") == result
    second = select_retained(source, reference, tmp_path / "two")
    from pathlib import Path

    first_run = Path(result["manifest"]).parent
    second_run = Path(second["manifest"]).parent
    assert json.loads((first_run / "subset.jsonl").read_text()) == product
    for name in ("subset.jsonl", "subset.parquet"):
        assert (first_run / name).read_bytes() == (second_run / name).read_bytes()
    assert (source / "current.json").read_bytes() == pointer
    (first_run / "subset.jsonl").write_text("corrupt")
    with pytest.raises(ValueError, match="corrupt"):
        select_retained(source, reference, tmp_path / "one")


@pytest.mark.parametrize(
    "change", ["missing", "duplicate", "changed", "checksum", "sources", "escape"]
)
def test_invalid_reference_never_publishes(tmp_path, change):
    source, reference, _ = fixture_catalog(tmp_path)
    meta = json.loads(reference.read_text())
    ids = reference.parent / meta["fingerprints_file"]
    with gzip.open(ids, "rt") as handle:
        item = json.loads(handle.read())
    if change == "missing":
        item["product_id"] = "amazon:B999999999"
    if change == "changed":
        item["line_sha256"] = "a" * 64
    with gzip.open(ids, "wt") as handle:
        handle.write(json.dumps(item) + "\n")
        if change == "duplicate":
            handle.write(json.dumps(item) + "\n")
    meta["fingerprints_sha256"] = digest(ids) if change != "checksum" else "b" * 64
    if change == "sources":
        meta["sources"][0]["sha256"] = "c" * 64
    if change == "escape":
        meta["fingerprints_file"] = "../ids.jsonl.gz"
    reference.write_text(json.dumps(meta))
    output = tmp_path / "selected"
    with pytest.raises(ValueError):
        select_retained(source, reference, output)
    assert not (output / "current.json").exists()


def test_overlapping_destination_is_rejected(tmp_path):
    source, reference, _ = fixture_catalog(tmp_path)
    with pytest.raises(ValueError, match="separate"):
        select_retained(source, reference, source)
