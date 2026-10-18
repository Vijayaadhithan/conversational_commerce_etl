# Conversational Commerce ETL

Independent preprocessing for historical Amazon fashion and beauty product
metadata. The pipeline audits every input record, normalizes supported fields,
deduplicates parent products, and builds a reproducible recommendation subset.

This repository contains **code, schemas, tests, and source metadata**. Product
archives, generated catalogs, reports, temporary databases, and `.env` remain
local and are excluded from Git. No models or embeddings are involved.

## Setup and run

Python 3.12+ is required. Keep the three previously downloaded metadata archives
in their existing location; they do not need to be copied into this project.

```bash
git clone https://github.com/Vijayaadhithan/conversational_commerce_etl.git
cd conversational_commerce_etl
bash scripts/setup.sh
```

Edit `.env` to point `RAW_DATA_DIR` at the archive directory. The default expects
the `conversational_commerce` backend next to this checkout. Existing `.env`
files are preserved, and newly created files have private permissions.

```bash
.venv/bin/python scripts/run_etl.py
.venv/bin/python scripts/verify_outputs.py
```

Optional overrides:

```bash
.venv/bin/python scripts/run_etl.py \
  --raw-dir /path/to/product/archives \
  --output-dir data/processed \
  --subset-size 20000 \
  --seed commerce-v1
```

Relative CLI and `.env` paths are resolved against the ETL project root. Input
and output directories must be separate, non-nested locations. The input archives
are read only. Their SHA-256 and expected row counts are pinned in `sources.json`.
If using another source revision, inspect it and explicitly update the manifest.

## Selected sources

| Source | Expected input records |
| --- | ---: |
| All Beauty | 112,590 |
| Amazon Fashion | 826,108 |
| Beauty & Personal Care | 1,028,914 |
| Total before deduplication | 1,967,612 |

These are product metadata from [Amazon Reviews 2023](https://amazon-reviews-2023.github.io/),
not the review archives. Metadata may include image URLs; this pipeline does not
download images. Prices are historical USD observations. It has no live stock,
current-price, purchasing, or safety validation. Check source terms before
redistributing third-party records.

## Outputs and versioning

Completed runs live under `data/processed/runs/<run_id>/`:

| File | Contents |
| --- | --- |
| `catalog.jsonl.gz` | Complete normalized catalog, one record per valid parent product ID |
| `subset.jsonl` | Up to 20,000 quality-eligible products with balanced strata |
| `data_quality.json` | Full record counts, field/type coverage, categories, rejected rows, conflicts, and subset composition |
| `data_quality.md` | Human-readable quality report |
| `rejects.jsonl` | Source/row/hash/reason for unusable records; original lines remain in the archives |
| `manifest.json` | Schema/pipeline/code versions, input checksums, parameters, output counts and checksums |

`data/processed/current.json` points to the latest successful run manifest.
Run IDs include source fingerprints, schema and processing code hashes, subset
size, and seed. Catalog/subset ordering and gzip headers are deterministic.
Timestamps are recorded only in the manifest, not the reproducible data outputs.

Publishing is atomic: an interrupted or failed run cannot replace the current
pointer. Incomplete work remains in `<run_id>.building` for inspection. Move that
directory aside explicitly before retrying the same run. Repeating a completed
run verifies its output checksums and reuses it. Use `--keep-work-db` to retain
the deduplication database for local inspection; normally it is removed after
successful export.

The pipeline streams records and uses a SQLite work database with a bounded
64 MiB page cache and disk-backed temporary storage. Allow disk headroom for the
database, exports, and sorting; this is intentionally not an in-memory ETL.

## Normalization rules

- Product identity is `amazon:<parent_asin>`. These are parent products, not
  guaranteed purchasable size/color variants. Different IDs with the same title
  are reported, not automatically merged.
- HTML and whitespace are cleaned. Title, descriptions, features, and observed
  detail claims retain source-row/field references for later explanations.
- Brand comes from the explicit Brand detail, not from a seller/store name.
- Positive, unambiguous USD prices are decimal strings. Missing, invalid,
  unsupported-currency, and range prices stay unknown, with parse status retained.
- Skin/hair type, materials, size, color, ingredients, and other detail attributes
  are extracted only when explicitly present. Department/age text is preserved;
  no gender, age suitability, ingredient benefit, or stock fact is invented.
- Fragrance-free is extracted only from an explicit, exact relevant detail claim.
  "Unscented" does not establish fragrance-free. Descriptive claims are not
  converted into independently verified safety/medical conclusions.
- Broad families use conservative category-path or title rules; evidence and
  derivation basis are explicit. Ambiguous/unmatched products remain unknown.
  This taxonomy is a baseline requiring evaluation, not human-labeled truth.

## Deduplication and subset policy

For duplicate IDs, one **complete observation** wins by information completeness,
then fixed source priority and stable content hash. Attributes are never stitched
across potentially different variants or historical observations. All source
observations remain traceable. Conflicting known prices, brands, sizes, colors,
materials, titles, and families are reported.

Subset eligibility requires a known broad category, a positive historical price,
descriptive content, and no cross-observation conflicts in price, family, brand,
size, color, or material. Unpriced and unclassified records remain in the full
catalog. Eligible products are sampled by seeded product-ID hash within selected
source/domain/family strata. Small-stratum capacity is redistributed, and
shortfalls are reported rather than filled with ineligible records.

The subset favors evidence-rich, priced records and balances families; it is
neither a representative prevalence sample nor a relevance benchmark.

## Backend handoff

The recommendation backend should consume a completed manifest and validate
checksums/schema version before importing records. Product claims have raw-source
references and an explicitly selected observation. The JSON Schema contract is
[schema/product-v1.schema.json](schema/product-v1.schema.json).

`scripts/verify_outputs.py` independently checks file checksums, every full-catalog
record's order/ID uniqueness and provenance counts, and JSON Schema compliance
for every subset record. It also checks each subset record is identical to its
canonical full-catalog record.

Catalog ingestion into PostgreSQL, embedding generation, vector search, and
conversation routing remain downstream work in the recommendation backend.

## Tests

```bash
.venv/bin/python -m pytest -q
```

Tests cover price parsing, unknown fields, conservative classification, source
evidence, duplicate conflicts, coherent winner selection, quarantine, schema
compliance, deterministic exports/subsets, integrity checks, and safe publication.
