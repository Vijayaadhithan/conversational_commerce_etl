import json
from decimal import Decimal

import duckdb
import pytest
from commerce_etl import pipeline
from commerce_etl.pipeline import digest, publication_lock, run_pipeline, verify_run
from commerce_etl.storage import PARQUET_VERSION
from test_pipeline import inputs, read_catalog, record


def rewrite_checksum(directory, name):
    manifest = json.loads((directory / "manifest.json").read_text())
    path = directory / name
    manifest["outputs"][name] = {"bytes": path.stat().st_size, "sha256": digest(path)}
    (directory / "manifest.json").write_text(json.dumps(manifest))


def test_parquet_explicit_decimals_nulls_and_lossless_documents(tmp_path):
    raw, source = inputs(
        tmp_path,
        {
            "All_Beauty": [
                record(price="12345678901234567890.12"),
                record("B000000002", price=None, details={}),
                record("B000000003", details={"Brand": "雪", "Size": "L", "Color": "Blue"}),
            ]
        },
    )
    directory, manifest = run_pipeline(raw, tmp_path / "out", source, 10, keep_work_db=True)
    assert manifest["pipeline_version"] == "2.0.0"
    assert manifest["parquet_version"] == PARQUET_VERSION
    assert manifest["engine"]["name"] == "duckdb"
    assert (directory / "work.duckdb").exists()
    assert not (directory / "normalized.jsonl").exists()
    assert not (directory / "canonical.jsonl").exists()
    with duckdb.connect() as db:
        rows = db.execute(
            "SELECT price_amount,brand,document_json FROM read_parquet(?) ORDER BY product_id",
            [str(directory / "catalog.parquet")],
        ).fetchall()
    assert rows[0][0] == Decimal("12345678901234567890.12")
    assert rows[1][:2] == (None, None)
    assert [json.loads(row[2]) for row in rows] == read_catalog(directory)
    assert verify_run(directory)["counts"] == manifest["counts"]


def test_empty_eligible_subset_has_typed_empty_parquet(tmp_path):
    raw, source = inputs(tmp_path, {"All_Beauty": [record(price=None)]})
    directory, manifest = run_pipeline(raw, tmp_path / "out", source, 10)
    assert manifest["counts"]["subset_rows"] == 0
    with duckdb.connect() as db:
        cursor = db.execute("SELECT * FROM read_parquet(?)", [str(directory / "subset.parquet")])
        assert cursor.fetchall() == []
        assert str(cursor.description[5][1]) == "DECIMAL(38,2)"
    verify_run(directory)


@pytest.mark.parametrize("name", ["catalog.parquet", "subset.parquet", "observations.parquet"])
def test_parquet_corruption_is_rejected(tmp_path, name):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    directory, _ = run_pipeline(raw, tmp_path / "out", source, 1)
    (directory / name).write_bytes(b"broken")
    with pytest.raises(ValueError, match="integrity"):
        verify_run(directory)


def test_semantic_drift_is_rejected_even_with_updated_checksum(tmp_path):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    directory, _ = run_pipeline(raw, tmp_path / "out", source, 1)
    path = directory / "catalog.parquet"
    with duckdb.connect() as db:
        db.execute("CREATE TABLE changed AS SELECT * FROM read_parquet(?)", [str(path)])
        db.execute("UPDATE changed SET price_amount=999")
        db.execute("COPY changed TO ? (FORMAT PARQUET)", [str(path)])
    rewrite_checksum(directory, "catalog.parquet")
    with pytest.raises(ValueError, match="differs from the canonical"):
        verify_run(directory)


def test_provenance_drift_is_rejected_even_with_updated_checksum(tmp_path):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    directory, _ = run_pipeline(raw, tmp_path / "out", source, 1)
    path = directory / "observations.parquet"
    with duckdb.connect() as db:
        db.execute("CREATE TABLE changed AS SELECT * FROM read_parquet(?)", [str(path)])
        db.execute("UPDATE changed SET record_sha256='wrong'")
        db.execute("COPY changed TO ? (FORMAT PARQUET)", [str(path)])
    rewrite_checksum(directory, "observations.parquet")
    with pytest.raises(ValueError, match="provenance differs"):
        verify_run(directory)


def test_verification_failure_keeps_previous_pointer(tmp_path, monkeypatch):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    output = tmp_path / "out"
    run_pipeline(raw, output, source, 1)
    previous = (output / "current.json").read_bytes()

    def fail(*args):
        raise ValueError("Injected Parquet verification failure")

    monkeypatch.setattr(pipeline, "verify_parquet", fail)
    with pytest.raises(ValueError, match="Injected"):
        run_pipeline(raw, output, source, 1, seed="new-generation")
    assert (output / "current.json").read_bytes() == previous
    assert len(list((output / "runs").glob("*.building"))) == 1


def test_concurrent_publication_is_rejected(tmp_path):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    output = tmp_path / "out"
    with publication_lock(output), pytest.raises(ValueError, match="Another ETL run"):
        run_pipeline(raw, output, source, 1)
    assert not (output / "current.json").exists()
    run_pipeline(raw, output, source, 1)


def test_changed_input_during_normalization_is_rejected(tmp_path, monkeypatch):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    real_rows = pipeline.input_rows

    def changed(path):
        yield from real_rows(path)
        with path.open("ab") as stream:
            stream.write(b"changed after reading")

    monkeypatch.setattr(pipeline, "input_rows", changed)
    with pytest.raises(ValueError, match="checksum changed"):
        run_pipeline(raw, tmp_path / "out", source, 1)
    assert not (tmp_path / "out/current.json").exists()


def test_deduplication_across_fetch_batches_preserves_complete_observation(tmp_path):
    duplicate = [record(price="10.00", details={"Brand": "Old"}) for _ in range(520)]
    raw, source = inputs(
        tmp_path,
        {
            "All_Beauty": duplicate + [record("B000000002")],
            "Beauty_and_Personal_Care": [
                record(price="20.00", details={"Brand": "New", "Size": "L"})
            ],
        },
    )
    directory, manifest = run_pipeline(raw, tmp_path / "out", source, 10)
    first = read_catalog(directory)[0]
    assert first["brand"]["value"] == "New"
    assert first["price"]["amount"] == "20.00"
    assert len(first["observations"]) == 521
    assert sum(item["selected"] for item in first["observations"]) == 1
    assert first["quality"]["conflicting_fields"] == ["brand", "price"]
    assert manifest["counts"]["subset_rows"] == 1
    quality = json.loads((directory / "data_quality.json").read_text())
    assert quality["sources"]["All_Beauty"]["duplicate_observations"] == 519
    assert quality["sources"]["Beauty_and_Personal_Care"]["duplicate_observations"] == 1


@pytest.mark.parametrize("version", [None, "", "unsupported"])
def test_missing_parquet_contract_is_rejected(tmp_path, version):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    directory, manifest = run_pipeline(raw, tmp_path / "out", source, 1)
    if version is None:
        del manifest["parquet_version"]
    else:
        manifest["parquet_version"] = version
    (directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Parquet output contract"):
        verify_run(directory)


def test_legacy_manifest_is_rejected(tmp_path):
    raw, source = inputs(tmp_path, {"All_Beauty": [record()]})
    directory, _ = run_pipeline(raw, tmp_path / "out", source, 1)
    manifest = json.loads((directory / "manifest.json").read_text())
    manifest["pipeline_version"] = "1.0.0"
    (directory / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="Unsupported pipeline"):
        verify_run(directory)
