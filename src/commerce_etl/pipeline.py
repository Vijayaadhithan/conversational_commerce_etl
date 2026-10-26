"""Streaming normalization, bulk DuckDB deduplication, verified Parquet and JSONL exports."""

import fcntl
import gzip
import hashlib
import json
import math
import shutil
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from commerce_etl import PIPELINE_VERSION, SCHEMA_VERSION
from commerce_etl.normalize import normalize
from commerce_etl.storage import (
    PARQUET_VERSION,
    batches,
    database,
    encode,
    export_parquet,
    prepare_products,
    stage_observation,
    verify_parquet,
)

MAX_LINE_BYTES = 8 * 1024 * 1024


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    Path(path).write_text(
        json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8"
    )


def atomic_json(path, value):
    temporary = Path(str(path) + ".tmp")
    write_json(temporary, value)
    temporary.replace(path)


@contextmanager
def deterministic_gzip(path):
    with Path(path).open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=3) as stream:
            yield stream


def invalid_constant(value):
    raise ValueError("non_standard_json_number")


def finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non_finite_json_number")
    return number


def input_rows(path):
    with gzip.open(path, "rb") as stream:
        row = 0
        while line := stream.readline(MAX_LINE_BYTES + 1):
            row += 1
            hasher = hashlib.sha256(line)
            if len(line) > MAX_LINE_BYTES:
                while not line.endswith(b"\n"):
                    line = stream.readline(MAX_LINE_BYTES + 1)
                    if not line:
                        break
                    hasher.update(line)
                yield row, None, hasher.hexdigest(), "record_too_large"
                continue
            try:
                value = json.loads(line, parse_constant=invalid_constant, parse_float=finite_float)
                yield row, value, hasher.hexdigest(), None
            except (ValueError, UnicodeDecodeError) as exc:
                yield row, None, hasher.hexdigest(), "invalid_json:" + type(exc).__name__


def source_audit():
    return {
        "input_rows": 0,
        "accepted_rows": 0,
        "rejected_rows": 0,
        "rejection_reasons": Counter(),
        "raw_field_presence": Counter(),
        "detail_key_presence": Counter(),
        "raw_field_types": {},
        "normalized_attribute_presence": Counter(),
        "classification": Counter(),
        "price_parse_status": Counter(),
        "duplicate_observations": 0,
    }


def audit_row(stats, record, product):
    for key, value in record.items():
        if value is not None and value != "" and value != [] and value != {}:
            stats["raw_field_presence"][key] += 1
        stats["raw_field_types"].setdefault(key, Counter())[type(value).__name__] += 1
    if isinstance(record.get("details"), dict):
        stats["detail_key_presence"].update(str(key) for key in record["details"])
    stats["normalized_attribute_presence"].update(
        key for key, value in product["attributes"].items() if value is not None
    )
    stats["classification"][product["category"]["basis"] or "unknown"] += 1
    stats["price_parse_status"][product["price"]["parse_status"]] += 1


def quotas_for(strata, total):
    """Equal coverage across nonempty strata, redistributing unused capacity."""
    quotas = {key: 0 for key in sorted(strata)}
    remaining = min(total, sum(strata.values()))
    while remaining:
        available = [key for key in quotas if quotas[key] < strata[key]]
        step = max(1, remaining // len(available))
        for key in available:
            take = min(step, remaining, strata[key] - quotas[key])
            quotas[key] += take
            remaining -= take
            if not remaining:
                break
    return quotas


def report_markdown(report):
    lines = [
        "# Catalog data-quality report",
        "",
        f"Run: `{report['run_id']}`",
        "",
        "Counts are measured over all input records, not a sample.",
        "",
        "| Source | Input rows | Accepted | Rejected | Known valid prices |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for source, stats in report["sources"].items():
        lines.append(
            f"| {source} | {stats['input_rows']:,} | {stats['accepted_rows']:,} | {stats['rejected_rows']:,} | {stats['price_parse_status'].get('valid', 0):,} |"
        )
    lines += [
        "",
        f"Unique parent product IDs: **{report['unique_products']:,}**.",
        f"Duplicate observations collapsed: **{report['duplicate_observations']:,}**.",
        f"Products with conflicting observations: **{report['conflicting_products']:,}**.",
        f"Eligible products: **{report['subset_eligible_products']:,}**; selected: **{report['subset_rows']:,}**.",
        "",
        "## Canonical catalog coverage",
        "",
        "| Domain | Family | Unique products | Known historical prices | Subset eligible |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in report["families"]:
        lines.append(
            f"| {row['domain']} | {row['family']} | {row['products']:,} | {row['priced']:,} | {row['eligible']:,} |"
        )
    lines += [
        "",
        "## Interpretation",
        "",
        "- Deduplication uses parent_asin, which identifies parent products, not guaranteed purchasable variants.",
        "- One complete observation wins deterministically; attributes/prices are not merged across observations.",
        "- Rule-derived broad categories are labeled as derived; they are not human-validated category labels.",
        "- Details and descriptions are listing claims, not independently verified suitability or safety facts.",
        "- Unknown prices/attributes remain unknown. Historical USD prices do not establish live stock or price.",
        "- The subset requires known category, positive historical price, descriptive content, and no critical cross-observation conflicts.",
        "- Subset sampling balances selected source/domain/family strata; it is not a prevalence estimate.",
        "- Field/type/attribute coverage uses accepted input rows as its denominator; rejected rows are counted separately.",
        "- Full field/type counts, rejection reasons, and conflicts are in data_quality.json.",
    ]
    return "\n".join(lines) + "\n"


def verify_run(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("pipeline_version") != PIPELINE_VERSION:
        raise ValueError("Unsupported pipeline version; regenerate with the DuckDB ETL")
    for name, info in manifest["outputs"].items():
        if Path(name).name != name or (directory / name).is_symlink():
            raise ValueError("Output filename must be a local basename")
        path = directory / name
        if (
            not path.is_file()
            or path.stat().st_size != info["bytes"]
            or digest(path) != info["sha256"]
        ):
            raise ValueError(f"Published output integrity failure: {name}")
    required = {
        "catalog.parquet", "subset.parquet", "observations.parquet",
        "catalog.jsonl.gz", "subset.jsonl", "data_quality.json", "data_quality.md", "rejects.jsonl",
    }
    if manifest.get("parquet_version") != PARQUET_VERSION or not required.issubset(manifest["outputs"]):
        raise ValueError("Missing or unsupported Parquet output contract")
    verify_parquet(directory, manifest)
    return manifest


@contextmanager
def publication_lock(output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    # Keep the lock inode in place; deleting it would allow concurrent lock holders.
    with (output_dir / ".etl.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another ETL run is publishing to this output directory") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def run_pipeline(
    raw_dir,
    output_dir,
    sources_path,
    subset_size=20000,
    seed="commerce-v1",
    keep_work_db=False,
    memory_limit="2GB",
    threads=1,
):
    raw_dir, output_dir = Path(raw_dir).resolve(), Path(output_dir).resolve()
    if output_dir == raw_dir or raw_dir in output_dir.parents or output_dir in raw_dir.parents:
        raise ValueError("Input and output directories must be separate, non-nested locations")
    if subset_size < 1:
        raise ValueError("Subset size must be positive")
    source_manifest = json.loads(Path(sources_path).read_text())
    if source_manifest.get("currency") != "USD":
        raise ValueError("This pipeline supports the selected historical USD metadata only")
    sources = sorted(source_manifest["sources"], key=lambda item: item["name"])
    if not sources:
        raise ValueError("At least one input source is required")
    if len({source["name"] for source in sources}) != len(sources):
        raise ValueError("Source names must be unique")
    print("Verifying input archive checksums", flush=True)
    inputs = []
    for source in sources:
        if Path(source["filename"]).name != source["filename"]:
            raise ValueError("Source filename must be a basename")
        path = raw_dir / source["filename"]
        actual = digest(path)
        if actual != source["sha256"]:
            raise ValueError(f"Input checksum mismatch: {source['name']}")
        inputs.append({**source, "bytes": path.stat().st_size})
    with publication_lock(output_dir):
        code_hash = hashlib.sha256()
        for file in sorted(Path(__file__).parent.glob("*.py")):
            code_hash.update(file.name.encode() + b"\0" + file.read_bytes())
        signature = {
            "schema_version": SCHEMA_VERSION,
            "pipeline_version": PIPELINE_VERSION,
            "code_sha256": code_hash.hexdigest(),
            "sources": sorted(inputs, key=lambda item: item["name"]),
            "schema_sha256": digest(
                Path(__file__).resolve().parents[2] / "schema/product-v1.schema.json"
            ),
            "subset_size": subset_size,
            "seed": seed,
            "retain_work_db": keep_work_db,
            "engine": {
                "name": "duckdb",
                "version": duckdb.__version__,
                "threads": threads,
                "memory_limit": memory_limit,
            },
            "parquet_version": PARQUET_VERSION,
        }
        run_id = hashlib.sha256(encode(signature).encode()).hexdigest()[:20]
        runs = output_dir / "runs"
        final = runs / run_id
        if final.exists():
            manifest = verify_run(final)
            if manifest.get("run_id") != run_id or any(
                manifest.get(key) != value for key, value in signature.items()
            ):
                raise ValueError("Published run metadata differs from the requested generation")
            atomic_json(
                output_dir / "current.json",
                {"run_id": run_id, "manifest": f"runs/{run_id}/manifest.json"},
            )
            print(f"Reusing verified run {run_id}", flush=True)
            return final, manifest
        runs.mkdir(parents=True, exist_ok=True)
        staging = runs / (run_id + ".building")
        if staging.exists():
            raise ValueError(
                f"Incomplete run exists at {staging}. Inspect or move it aside before retrying."
            )
        staging.mkdir()
        db = database(staging / "work.duckdb", memory_limit, threads)
        audits = {source["name"]: source_audit() for source in sources}
        accepted = 0
        try:
            with (
                (staging / "rejects.jsonl").open("w", encoding="utf-8") as rejects,
                (staging / "normalized.jsonl").open("w", encoding="utf-8") as normalized,
            ):
                for source in sources:
                    name = source["name"]
                    stats = audits[name]
                    print(f"Processing {name}", flush=True)
                    for row, record, sha, reason in input_rows(raw_dir / source["filename"]):
                        stats["input_rows"] += 1
                        product = None
                        if reason is None:
                            try:
                                product = normalize(record, name, row, sha)
                            except ValueError as exc:
                                reason = str(exc)
                        if reason:
                            stats["rejected_rows"] += 1
                            stats["rejection_reasons"][reason] += 1
                            rejects.write(
                                encode(
                                    {
                                        "source": name,
                                        "row": row,
                                        "record_sha256": sha,
                                        "reason": reason,
                                    }
                                )
                                + "\n"
                            )
                            continue
                        stats["accepted_rows"] += 1
                        audit_row(stats, record, product)
                        stage_observation(normalized, product)
                        accepted += 1
                        if accepted % 100000 == 0:
                            print(f"Accepted {accepted:,} records", flush=True)
                    if (
                        source.get("expected_rows") is not None
                        and stats["input_rows"] != source["expected_rows"]
                    ):
                        raise ValueError(f"Unexpected row count in {name}")
            for source in inputs:
                if digest(raw_dir / source["filename"]) != source["sha256"]:
                    raise ValueError("Input checksum changed during normalization")
            if not accepted:
                raise ValueError("No valid products found; no run was published")
            print("Bulk loading and deduplicating with DuckDB", flush=True)
            duplicate_counts = prepare_products(db, staging, seed)
            for name, stats in audits.items():
                stats["duplicate_observations"] = duplicate_counts.get(name, 0)
            unique = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            if not unique:
                raise ValueError("No valid products found; no run was published")
            conflicting = db.execute(
                "SELECT COUNT(*) FROM products WHERE conflicts!='[]'"
            ).fetchone()[0]
            conflict_counts = Counter()
            for (payload,) in batches(
                db.execute("SELECT conflicts FROM products WHERE conflicts!='[]'")
            ):
                conflict_counts.update(json.loads(payload))
            families = [
                {"domain": d, "family": f, "products": n, "priced": p, "eligible": e}
                for d, f, n, p, e in db.execute(
                    "SELECT domain,family,COUNT(*),SUM(price_known),SUM(subset_eligible) FROM products GROUP BY domain,family ORDER BY domain,family"
                ).fetchall()
            ]
            # Source attribution is the selected complete observation, not a manufactured merged record.
            strata_rows = db.execute("""SELECT selected_source,domain,family,COUNT(*)
                                       FROM products WHERE subset_eligible=1 GROUP BY 1,2,3 ORDER BY 1,2,3""").fetchall()
            strata = {(s, d, f): n for s, d, f, n in strata_rows}
            quotas = quotas_for(strata, subset_size)
            db.execute("CREATE TEMP TABLE selected (product_id TEXT PRIMARY KEY)")
            for (source, domain, family), quota in quotas.items():
                db.execute(
                    """INSERT INTO selected SELECT product_id FROM products WHERE subset_eligible=1 AND domain=? AND family=?
                              AND selected_source=? ORDER BY subset_rank,product_id LIMIT ?""",
                    (domain, family, source, quota),
                )
            subset_rows = db.execute("SELECT COUNT(*) FROM selected").fetchone()[0]
            title_groups, title_products = db.execute(
                "SELECT COUNT(*),COALESCE(SUM(n),0) FROM (SELECT COUNT(*) n FROM products GROUP BY normalized_title HAVING COUNT(*)>1)"
            ).fetchone()
            print(f"Exporting {unique:,} products and {subset_rows:,} subset rows", flush=True)
            subset_counts = Counter()
            with (
                deterministic_gzip(staging / "catalog.jsonl.gz") as catalog,
                (staging / "subset.jsonl").open("w", encoding="utf-8") as subset,
            ):
                exported = 0
                for payload, is_selected in batches(
                    db.execute(
                        "SELECT p.payload,s.product_id IS NOT NULL FROM products p LEFT JOIN selected s ON s.product_id=p.product_id ORDER BY p.product_id"
                    )
                ):
                    product = json.loads(payload)
                    encoded = encode(product) + "\n"
                    catalog.write(encoded.encode())
                    if is_selected:
                        subset.write(encoded)
                        key = (
                            product["selected_observation"]["source"],
                            product["category"]["domain"],
                            product["category"]["family"],
                        )
                        subset_counts[key] += 1
                    exported += 1
                    if exported % 200000 == 0:
                        print(f"Exported {exported:,} products", flush=True)
            export_parquet(db, staging)
            report = {
                "run_id": run_id,
                "schema_version": SCHEMA_VERSION,
                "sources": audits,
                "input_rows": sum(s["input_rows"] for s in audits.values()),
                "accepted_rows": accepted,
                "rejected_rows": sum(s["rejected_rows"] for s in audits.values()),
                "unique_products": unique,
                "duplicate_observations": accepted - unique,
                "conflicting_products": conflicting,
                "conflicting_field_counts": conflict_counts,
                "families": families,
                "same_title_different_ids": {"groups": title_groups, "products": title_products},
                "subset_eligible_products": sum(strata.values()),
                "subset_rows": subset_rows,
                "subset_requested_rows": subset_size,
                "subset_strata": [
                    {
                        "source": s,
                        "domain": d,
                        "family": f,
                        "eligible": strata[(s, d, f)],
                        "selected": subset_counts[(s, d, f)],
                    }
                    for s, d, f in sorted(strata)
                ],
                "subset_shortfall": max(0, subset_size - subset_rows),
            }
            write_json(staging / "data_quality.json", report)
            (staging / "data_quality.md").write_text(report_markdown(report), encoding="utf-8")
            manifest = {
                **signature,
                "run_id": run_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "counts": {
                    key: report[key]
                    for key in (
                        "input_rows",
                        "accepted_rows",
                        "rejected_rows",
                        "unique_products",
                        "subset_rows",
                    )
                },
                "outputs": {
                    name: {
                        "bytes": (staging / name).stat().st_size,
                        "sha256": digest(staging / name),
                    }
                    for name in (
                        "catalog.jsonl.gz",
                        "subset.jsonl",
                        "data_quality.json",
                        "data_quality.md",
                        "rejects.jsonl",
                        "catalog.parquet",
                        "subset.parquet",
                        "observations.parquet",
                    )
                },
            }
            write_json(staging / "manifest.json", manifest)
            verify_parquet(staging, manifest, db)
            db.execute("CHECKPOINT")
            db.close()
            db = None
            if not keep_work_db:
                for suffix in ("", ".wal"):
                    (staging / ("work.duckdb" + suffix)).unlink(missing_ok=True)
            spill = staging / "duckdb_spill"
            if spill.exists():
                shutil.rmtree(spill)
            staging.replace(final)
            atomic_json(
                output_dir / "current.json",
                {"run_id": run_id, "manifest": f"runs/{run_id}/manifest.json"},
            )
            print(f"Published run {run_id}", flush=True)
            return final, manifest
        finally:
            if db is not None:
                db.close()
