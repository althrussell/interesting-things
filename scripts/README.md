# Scripts

## `data_classification_cost_analysis.py`

A Databricks notebook (source format — import via **Workspace → Import → File**) that gives
visibility into what is driving **Unity Catalog automatic data classification** spend.

Data Classification runs on serverless compute and is billed under
`billing_origin_product = 'DATA_CLASSIFICATION'` in `system.billing.usage`. It scans more than
just brand-new tables — initial scans, manual full rescans, and (most commonly) tables that get
**overwritten/recreated by ETL** all trigger re-classification. This notebook finds which of
those is happening and where.

### What it does

| Cell | Output |
|------|--------|
| 1 | Monthly DBUs + estimated $ for classification |
| 2 | Daily trend — spike vs. steady-state |
| 3 | Biggest days, flagged at ≥3× the daily average |
| 4a/4b | Attribution by **catalog / workspace / principal**, with names resolved |
| 5 | Why cost attribution stops at the catalog level (metadata reference) |
| 6 | Per-table scan status (`__table_statuses`) — last scan, errors |
| 7 | Per-table classification results (`results`) — what's being tagged |
| 8 | Full-rescan vs. incremental, **inferred** from scan-time clustering |
| 9 / 9b | **dbt full-reload detection** via `system.query.history` — models run as `CREATE OR REPLACE TABLE` get re-scanned every run |

### Parameters (widgets)

- `lookback_days` — analysis window (default 90)
- `discount_rate` — price multiplier for $ estimates (1.0 = list price)
- `catalog_filter` — optional single `catalog_id` to focus on

### Access required (a metastore/account admin grants these)

| Grant | Enables |
|-------|---------|
| `SELECT` on `system.billing` | Cost queries (cells 1–5). Required. |
| `SELECT` on `system.access` | Workspace **names** |
| `SELECT` on `system.data_classification` | Catalog **names** + table-level detail (cells 6–8) |
| `SELECT` on `system.query` | dbt full-reload detection (cells 9/9b) |
| Directory read (SCIM) | Resolves the `run_as` service-principal GUID to a name |

Every name/table-level cell degrades gracefully — if a grant is missing it prints a note and the
rest of the notebook still runs.

### Notes

- Dollar figures are **estimates** from `system.billing.list_prices` × your discount rate; they
  won't exactly match an invoice but are accurate for finding the cost driver.
- Attach to a **serverless SQL warehouse**.
- Classification cost is only attributable to the **catalog** level (billing carries `catalog_id`
  but no schema/table id). Table-level visibility comes from `system.data_classification` and
  `system.query.history`, not billing.
