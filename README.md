# Parcelpath

A working retail operations data product built with standard-library Python and SQL. Messy CSV/JSONL source data becomes a tested SQLite warehouse, an analytical mart, a JSON API and an interactive web app - no dependencies, no API keys, no paid services.

**Data is synthetic.** The seeded dataset (150 customers, 40 products, 2,400 orders, 4 deliberately broken records) is generated deterministically. No real business data is used anywhere.

## The product

The web app (`python3 parcelpath.py serve`) is a live operations studio over the real warehouse:

- **Overview** - filterable KPIs (orders, net merchandise value, on-time delivery, in transit, quarantined records), an interactive daily trend chart, city/category breakdowns that filter on click, and live warehouse health.
- **Pipeline** - run the real batch loader from the browser and watch per-file results and the 8-check quality suite stream in. Unchanged files are skipped by SHA-256 replay guard.
- **Upload data** - drag in your own `orders-*.jsonl`, `shipments-*.csv`, `customers-*.csv` or `products-*.csv` batches. They load through the same validation, atomic loads and quarantine as the CLI. Upload the same file twice to see the replay guard skip it. Sample batches (including a deliberately messy one) are downloadable in-app.
- **Explorer** - paginated, filterable order browser plus the quarantine log (rejected record, line number, exact reason, payload) and the full load manifest.
- **SQL console** - read-only SELECT/WITH queries against the live warehouse, capped at 1,000 rows, with example queries.

## Run it locally in two minutes

```sh
python3 parcelpath.py demo     # generate data, load warehouse, run checks, export reports
python3 -m unittest            # 21 automated tests
python3 parcelpath.py serve    # web app on http://127.0.0.1:8000
```

Python 3.10+ is the only requirement. The server binds to localhost by default.

## Hosted demo

Public app, no login. Each browser gets an isolated temporary SQLite warehouse, raw-batch folder, pipeline status and write lock. A random session identifier is carried in an HMAC-signed, HttpOnly, SameSite=Strict cookie (Secure on the host). Uploads, resets and pipeline runs affect only that browser's workspace, never the shared seed or another visitor. Treat the cookie like a temporary access key; anyone using the same browser profile shares its workspace.

The demo allows up to 20 visitor workspaces at once. Idle workspaces expire after two hours and are cleaned up on the next visit. Each workspace allows 60 actions/hour, files up to 1 MB, at most 40 batches and a 5 MB raw-data cap. SQL is read-only, capped at 1,000 rows with a two-second computation budget and bounded value/response sizes. Cross-origin actions and unauthenticated POSTs without a valid workspace cookie are rejected. These are demo limits, not production abuse protection.

The free host sleeps when idle and its disk is temporary. Workspaces reset on restart, redeploy, expiry or cookie loss. Capacity limits may temporarily prevent new visitors. Use only synthetic data, never personal or sensitive records. This is a demo, not permanent storage.

## Commands

```sh
python3 parcelpath.py load --raw data/raw --db data/warehouse/retail.db
python3 parcelpath.py check
python3 parcelpath.py export --out reports
python3 parcelpath.py query --sql-file analysis.sql
python3 parcelpath.py serve --port 8000 --host 127.0.0.1
```

For new input, add immutable batches named `orders-002.jsonl` or `shipments-003.csv` to `data/raw/`, then run `load`, `check` and `export` - or just drop them in the Upload tab. Customer/product batches load before order/shipment batches. Unknown foreign keys are quarantined, not automatically retried: fix the reference and reprocess a corrected batch. Never delete or modify a production source file in a real pipeline.

The `query` command and the SQL console both use read-only SQLite connections and cap results at 1,000 rows. The API binds filter values as parameters, never SQL fragments.

## Business definitions

- Net merchandise value = gross value minus discounts, **not** recognized revenue or profit. Excludes returns, tax and shipping.
- On-time delivery = delivered on/before promise divided by delivered orders. In-transit orders are excluded from that denominator.
- In transit = missing delivered date in the source snapshot, not a live carrier status.
- Seeded dates cover 1 Aug - 29 Oct 2026. These are synthetic historical examples.

## Architecture and trade-offs

```
CSV + JSONL immutable batches
        |
Python validation + record quarantine
        |
SQLite dimension/fact warehouse + load manifests
        |
SQL mart_order + reconciliation checks
        |
JSON/CSV reports + JSON API + interactive web app
```

Dimensions use **SCD Type 1** (latest loaded attributes overwrite earlier ones). Facts use source-version guards: a newer `updated_at` wins, stale events cannot overwrite current facts, and equal-timestamp conflicts are first-write-wins. Each file loads in one transaction with per-record savepoints; a file-level failure rolls back the whole batch, while a bad record rolls back only itself and lands in quarantine with source hash, line number, reason and payload. A SHA-256 manifest match skips already processed files, so replays are safe. This is an idempotent batch demonstration, not distributed exactly-once processing.

Money is integer paise end to end. Dates are strict ISO days; source versions are canonical UTC timestamps so string ordering is safe.

### Quality and tests

Eight warehouse checks: foreign keys, non-empty orders, unique order grain, non-negative net value, valid delivery days, late-is-a-subset-of-delivered, mart-to-fact row reconciliation and money reconciliation. Twenty-one automated tests cover replay idempotency, stale/new update ordering, invalid record isolation, whole-file rollback on failure, late shipment corrections, export reconciliation, empty-warehouse failure, API metric filters, pagination, the SQL guard, upload filename validation sample-batch ingestion, signed session cookies, upload/reset isolation between visitors, separate pipeline status, cross-origin rejection and idle cleanup.

### Known limits

Single-process SQLite; no orchestration, scheduler, CDC, distributed compute, per-user accounts or PII controls. CSV parse errors fail the whole file rather than creating row quarantines. Batches are materialized in memory, so this is not a large-scale benchmark. Dimensions have no version guards; replay source order is part of the demonstration contract. API aggregates materialize filtered rows; use SQL-side aggregation for larger datasets. The hosted demo has temporary cookie-scoped workspaces, not permanent accounts. It is not designed for sensitive data or high traffic. Clearing cookies starts a new workspace.

`parcelpath.py` is deliberately self-contained: schema and the web app are embedded so downloading the single file is enough to run everything. `schema.sql` and `dashboard.html` are readable copies of those embedded assets, not separate runtime dependencies. Keep embedded and standalone copies aligned when editing.

## A truthful description

"Built a Python/SQL retail analytics product with synthetic data: replay-safe batch ingestion, a relational warehouse with a rejected-record quarantine, automated quality checks, and an interactive web app with live pipeline runs, uploads, an order explorer and a read-only SQL console."
