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
