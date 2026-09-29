# Column Masking — Query Performance Diagnostics

Tools to find whether Unity Catalog **column masks** are actually hurting query performance, and to
pinpoint the tables and mask functions that cost the most.

## Contents
- **`Column_Masking_Performance_Guide.pdf`** — short guide: how masks affect performance, how to run the
  notebook, how to read the results, and best practices (with Databricks documentation links).
- **`Column_Masking_Performance_Diagnostic.ipynb`** — the diagnostic notebook (Jupyter).
- **`Column_Masking_Performance_Diagnostic.sql`** — the same notebook in Databricks source format.

## What the notebook does
Ranks the tables and mask functions that consume the most query time (from `system.query.history` joined to
`system.access.table_lineage`), flags Python / non-deterministic masks, lists the slowest queries with their
bottleneck signals (spill / shuffle / cache / file pruning), and reads a query plan (`EXPLAIN FORMATTED`) to
show whether masking is in the critical path.

## How to run
Import the `.ipynb` or `.sql` into Databricks and run it on a **SQL warehouse**. It is **100% read-only**
(`SELECT` / `EXPLAIN` against `system.*` only — it creates nothing). Requires the `system.query`,
`system.access`, and `system.information_schema` system schemas. Start with the default 3-day window and
widen it once it's responsive.

## Key takeaways
- SQL column masks are near-free (~1% over a plain read); Python UDF masks are ~17× slower and cannot run on
  serverless (they require classic clusters).
- Mask **count** is not the same as query **cost** — masks fire only on the masked columns a query selects.
- If the notebook shows no Python / non-deterministic masks, masking is not your bottleneck — use the guide
  to chase the real cause (table statistics, data layout, shuffle/spill, warehouse sizing).

## DBSQL RightSize Advisor (AI/BI dashboard)

`DBSQL_RightSize_Advisor.lvdash.json` — an AI/BI (Lakeview) dashboard that reviews your SQL
warehouses from your **own system tables** and recommends right-sizing actions.

**Import:** In Databricks, go to **Dashboards → Import dashboard from file**, select this file, then
pick a SQL warehouse to run it on.

**What it shows:** KPIs (queries, spill, queue, cold-start over 7 days); a **right-size recommendation
table** (upsize / downsize / go-serverless / adjust auto-stop per warehouse); a daily query-volume and
queue-pressure trend; per-warehouse health; and the longest-running queries. A warehouse filter is on the
Filters page.

**Requires:** the `system.query` and `system.compute` system schemas (standard on Unity Catalog). It is
read-only and creates nothing.

## Platform Health Check (notebook)

`Platform_Health_Check.ipynb` (Jupyter) / `Platform_Health_Check.py` (Databricks source) — a self-service
platform maturity review that reads your **own system tables** and produces a single **ranked list of things
to fix** (P1 / P2 / P3), each with evidence, the affected object and a recommended action.

**What it checks:** table inventory (Delta, managed vs external, Hive metastore); Hive metastore and
path-based access; hot tables by read/write volume; a per-table deep dive (file sizes, clustering,
predictive optimisation, OPTIMIZE/VACUUM history); predictive optimisation failures; ingestion patterns;
compute (runtime end-of-support, access modes, policies, utilisation); jobs (failures, skipped runs, cost,
bundles, health rules); SQL warehouses and query performance by client; and spend at list price.

**How to run:** import into Databricks, attach to **serverless** (or a UC cluster on DBR 15.4 LTS+), set the
widgets (`workspace_ids` = `all` | `current` | comma list, `lookback_days`, `top_n_tables`) and Run all.
Every system-table query is date- and workspace-scoped; typical runtime is 5–15 minutes. On large estates,
use the optional `catalogs` and `schemas` widgets (comma lists; a schema can be `schema` or `catalog.schema`)
to limit the table-level checks (inventory, hot tables, deep dive, predictive optimisation) to part of the
estate. Compute, jobs, warehouse and billing checks are workspace-level and ignore them. It is **read-only**
(`SELECT` / `SHOW` / `DESCRIBE`) unless you set `output_table` to keep a history of findings. Missing
system schemas or permissions skip the affected check and are listed in the check log.

**Requires:** the `access`, `query`, `compute`, `lakeflow`, `storage` and `billing` system schemas. Run as an
admin for full coverage: query text is redacted for non-privileged users.
