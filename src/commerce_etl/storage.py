"""Bulk DuckDB staging and explicit, lossless Parquet export contracts."""

import gzip
import hashlib
import json
from decimal import Decimal
from itertools import groupby
from pathlib import Path

import duckdb

from commerce_etl.normalize import conflict_values, eligible

SOURCE_PRIORITY = {"All_Beauty": 1, "Amazon_Fashion": 2, "Beauty_and_Personal_Care": 3}
PARQUET_VERSION = "1.0.0"
OBSERVATION_COLUMNS = {
    "product_id": "VARCHAR",
    "source": "VARCHAR",
    "row_number": "BIGINT",
    "record_sha256": "VARCHAR",
    "payload": "VARCHAR",
}
PRODUCT_COLUMNS = {
    "product_id": "VARCHAR",
    "payload": "VARCHAR",
    "conflicts": "VARCHAR",
    "domain": "VARCHAR",
    "family": "VARCHAR",
    "subset_eligible": "BOOLEAN",
    "subset_rank": "VARCHAR",
    "price_known": "BOOLEAN",
    "normalized_title": "VARCHAR",
    "selected_source": "VARCHAR",
}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def database(path, memory_limit="2GB", threads=1):
    # Values are configuration, never interpolated SQL identifiers or paths.
    if not 1 <= threads <= 32:
        raise ValueError("DuckDB threads must be between 1 and 32")
    db = duckdb.connect(str(path), config={"memory_limit": memory_limit, "threads": threads})
    db.execute("SET preserve_insertion_order=true")
    db.execute("SET temp_directory = ?", [str(Path(path).parent / "duckdb_spill")])
    return db


def batches(cursor, size=512):
    while rows := cursor.fetchmany(size):
        yield from rows


def stage_observation(stream, product):
    observation = product["selected_observation"]
    stream.write(
        encode(
            {
                "product_id": product["product_id"],
                "source": observation["source"],
                "row_number": observation["row"],
                "record_sha256": observation["record_sha256"],
                "payload": encode(product),
            }
        )
        + "\n"
    )


def load_json_table(db, name, path, columns):
    # Only module-owned table names are allowed. Explicit types prevent inference drift.
    if name not in {"observations", "products"}:
        raise ValueError("Unknown staging table")
    relation = db.read_json(
        str(path), columns=columns, format="newline_delimited", maximum_object_size=64 * 1024 * 1024
    )
    relation.create(name)


def winner_key(product):
    observation = product["selected_observation"]
    tie = f"{SOURCE_PRIORITY.get(observation['source'], 0)}:{observation['record_sha256']}:{observation['row']:012}"
    return product["quality"]["score"], tie


def canonical_products(db):
    # DuckDB sorts/spills to disk. Python holds a batch and one product's provenance only.
    rows = batches(
        db.execute(
            "SELECT product_id,payload FROM observations ORDER BY product_id,source,row_number"
        )
    )
    for _, grouped in groupby(rows, key=lambda row: row[0]):
        winner, seen, observations = None, {}, []
        for _, payload in grouped:
            product = json.loads(payload)
            observations.append(product["selected_observation"])
            for key, value in conflict_values(product).items():
                if value is not None:
                    seen.setdefault(key, set()).add(str(value).casefold())
            if winner is None or winner_key(product) > winner_key(winner):
                winner = product
        conflicts = sorted(key for key, values in seen.items() if len(values) > 1)
        winner["quality"]["conflicting_fields"] = conflicts
        winner["quality"]["eligible_for_subset"] = eligible(winner, conflicts)
        if conflicts and "conflicting_observations" not in winner["quality"]["warnings"]:
            winner["quality"]["warnings"].append("conflicting_observations")
        selected = winner["selected_observation"]
        winner["observations"] = [{**item, "selected": item == selected} for item in observations]
        yield winner


def prepare_products(db, staging, seed):
    load_json_table(db, "observations", staging / "normalized.jsonl", OBSERVATION_COLUMNS)
    # Duplicates attributed to the same source as the original sorted streaming pipeline.
    duplicate_counts = dict(
        db.execute("""SELECT source,count(*) FROM (
        SELECT source,row_number() OVER(PARTITION BY product_id ORDER BY source,row_number) AS occurrence
        FROM observations) WHERE occurrence>1 GROUP BY source""").fetchall()
    )
    path = staging / "canonical.jsonl"
    with path.open("w", encoding="utf-8") as stream:
        for product in canonical_products(db):
            stream.write(
                encode(
                    {
                        "product_id": product["product_id"],
                        "payload": encode(product),
                        "conflicts": encode(product["quality"]["conflicting_fields"]),
                        "domain": product["category"]["domain"],
                        "family": product["category"]["family"],
                        "subset_eligible": product["quality"]["eligible_for_subset"],
                        "subset_rank": hashlib.sha256(
                            (seed + "\0" + product["product_id"]).encode()
                        ).hexdigest(),
                        "price_known": product["price"]["amount"] is not None,
                        "normalized_title": product["title"].casefold(),
                        "selected_source": product["selected_observation"]["source"],
                    }
                )
                + "\n"
            )
    load_json_table(db, "products", path, PRODUCT_COLUMNS)
    (staging / "normalized.jsonl").unlink()
    path.unlink()
    return duplicate_counts


def export_parquet(db, directory):
    # Dynamic claim values remain in document_json; typed columns support analytics.
    # Strict DECIMAL casts fail publication instead of silently rounding through float.
    db.execute("""CREATE VIEW parquet_products AS SELECT
        product_id,json_extract_string(payload,'$.parent_asin') AS parent_asin,
        json_extract_string(payload,'$.title') AS title,domain,family,
        CAST(json_extract_string(payload,'$.price.amount') AS DECIMAL(38,2)) AS price_amount,
        json_extract_string(payload,'$.price.currency') AS currency,
        json_extract_string(payload,'$.brand.value') AS brand,
        selected_source,subset_eligible AS eligible_for_subset,
        payload AS document_json FROM products""")
    for name, query in (
        ("catalog.parquet", "SELECT * FROM parquet_products ORDER BY product_id"),
        (
            "subset.parquet",
            "SELECT p.* FROM parquet_products p JOIN selected s USING(product_id) ORDER BY product_id",
        ),
        (
            "observations.parquet",
            "SELECT product_id,source,row_number,record_sha256 FROM observations ORDER BY product_id,source,row_number",
        ),
    ):
        db.execute(
            f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 8192)",
            [str(directory / name)],
        )


def verify_parquet(directory, manifest, db=None):
    """Compare every Parquet document with JSONL and every flattened provenance row."""
    owned = db is None
    db = db or duckdb.connect(config={"threads": 1, "memory_limit": "2GB"})
    try:
        for name, source, count_key in (
            ("catalog", "catalog.jsonl.gz", "unique_products"),
            ("subset", "subset.jsonl", "subset_rows"),
        ):
            cursor = db.execute(
                "SELECT * FROM read_parquet(?) ORDER BY product_id",
                [str(directory / (name + ".parquet"))],
            )
            columns = [column[0] for column in cursor.description]
            expected_columns = [
                "product_id",
                "parent_asin",
                "title",
                "domain",
                "family",
                "price_amount",
                "currency",
                "brand",
                "selected_source",
                "eligible_for_subset",
                "document_json",
            ]
            types = [str(column[1]) for column in cursor.description]
            if columns != expected_columns or types != ["VARCHAR"] * 5 + ["DECIMAL(38,2)"] + [
                "VARCHAR"
            ] * 3 + ["BOOLEAN", "VARCHAR"]:
                raise ValueError("Parquet column contract differs")
            rows = iter(batches(cursor))
            opener = gzip.open if source.endswith(".gz") else open
            count = 0
            previous = ""
            with opener(directory / source, "rt", encoding="utf-8") as stream:
                for line in stream:
                    product = json.loads(line)
                    row = next(rows, None)
                    expected = (
                        product["product_id"],
                        product["parent_asin"],
                        product["title"],
                        product["category"]["domain"],
                        product["category"]["family"],
                        Decimal(product["price"]["amount"])
                        if product["price"]["amount"] is not None
                        else None,
                        product["price"]["currency"],
                        product["brand"]["value"] if product["brand"] else None,
                        product["selected_observation"]["source"],
                        product["quality"]["eligible_for_subset"],
                        line.rstrip("\n"),
                    )
                    if row != expected or product["product_id"] <= previous:
                        raise ValueError(
                            "Parquet product differs from the canonical JSONL contract"
                        )
                    previous = product["product_id"]
                    count += 1
            if next(rows, None) is not None or count != manifest["counts"][count_key]:
                raise ValueError("Parquet product counts differ")
        provenance = iter(
            batches(
                db.execute(
                    "SELECT product_id,source,row_number,record_sha256 FROM read_parquet(?) ORDER BY product_id,source,row_number",
                    [str(directory / "observations.parquet")],
                )
            )
        )
        count = 0
        with gzip.open(directory / "catalog.jsonl.gz", "rt", encoding="utf-8") as stream:
            for line in stream:
                product = json.loads(line)
                for item in product["observations"]:
                    if next(provenance, None) != (
                        product["product_id"],
                        item["source"],
                        item["row"],
                        item["record_sha256"],
                    ):
                        raise ValueError("Parquet provenance differs from the canonical catalog")
                    count += 1
        if next(provenance, None) is not None or count != manifest["counts"]["accepted_rows"]:
            raise ValueError("Parquet observation counts differ")
    finally:
        if owned:
            db.close()
