# Lakehouse Performance Diagnostics

Read-only SQL tools and a best-practice guide for finding, fixing and preventing slow queries on Databricks:
data files and layout, Delta maintenance, column masks and row filters, queries, and SQL warehouses.

## Contents
- **`Lakehouse_Performance_Best_Practices.pdf`**: field guide with the rule, why it matters, how to check it
  and how to fix it, for files, liquid clustering and partitioning, OPTIMIZE / VACUUM / ANALYZE, masks
  (including ABAC policies on governed tags), queries and warehouses. Also a "don't do this again" list,
  daily early-warning alerts and a production-readiness process. Every SQL statement in it was tested
  end-to-end.
- **`Column_Masking_Performance_Diagnostic`** (`.sql` / `.ipynb`): where query time goes, by table, mask
  function and query, split into **planning** and **execution**.
- **`Production_Readiness_Check`** (`.sql` / `.ipynb`): a PASS / WARN / FAIL readiness gate for a new data
  product, run before go-live and again 2–4 weeks after.
- **`Performance_Alerts`** (`.sql` / `.ipynb`): five daily early-warning SQL alert queries with setup steps
  and a calibration query.
- **`DBSQL_RightSize_Advisor.lvdash.json`** and **`Platform_Health_Check`**: see below.

The `.sql` files are Databricks source format and the `.ipynb` files are the same notebooks as Jupyter.
Import either with **Workspace → Import → File** and attach a **SQL warehouse**. All three SQL notebooks
are read-only against `system.*` (the readiness check writes only if you set `results_table`).

## Key takeaways: column masks
- A mask costs time in two ways. **Execution** is per row, only for the masked columns a query selects:
  small for SQL masks, ~17× more for Python masks, which also can't be masks on serverless.
  **Planning** applies when a mask **reads a table** (for example a permission lookup). It is paid by *every*
  query on the table, for *every* mask on it, whatever columns are selected. We measured about 0.16 s per
  such mask on a SQL warehouse and 0.65 s on serverless; a 52-mask table planned for 34–39 s on serverless.
- Narrow views, column lists, `LIMIT`, bypass-group membership and the result cache don't remove planning
  cost. Removing the lookup does: **ABAC policies on governed tags** (~1.1 s on the same table) or masks that
  only check `is_account_group_member()` / `current_user()` (~1.5 s).
- *"Databricks recommends ABAC policies when you need consistent row filtering and column masking across
  many tables."*
- Rank queries by **total** time and compare `compilation_duration_ms` with `execution_duration_ms`.
  Planning-bound queries don't show up in rankings by execution time.

## Column_Masking_Performance_Diagnostic
Sections: (1) tables that cost the most time, with average planning vs execution and lookup-mask count;
(2) mask functions on the busiest tables, with `sql_data_access`; (3) the expensive kinds of mask function
(table lookup, Python, non-deterministic); (4) estimated vs measured planning cost per masked table;
(5) planning-bound queries; (6) slowest queries and their bottleneck signals; (7) mask density;
(8) Predictive Optimization and maintenance on the busiest tables.
Default look-back is 3 days (edit `INTERVAL 3 DAYS` inline). Requires `system.query`, `system.access`,
`system.information_schema` and `system.storage`.

## Production_Readiness_Check
Set `target_catalog` and `target_schemas` in the first cell and run after a representative test workload.
It checks masks and row filters (table-lookup masks fail at 10+ per table; Python and non-deterministic
masks; per-table masks where ABAC policies would scale), maintenance, partitioning, query latency and
planning, and the warehouses used, then gives a verdict: **READY**, **READY WITH WARNINGS** or
**NOT READY**. A guided last step lists `DESCRIBE DETAIL` / `DESCRIBE SCHEMA EXTENDED` commands and what
healthy output looks like (clustering, file size, VACUUM retention, Predictive Optimization).
Requires `system.query`, `system.access`, `system.storage`, `system.compute` and
`system.information_schema`.

## Performance_Alerts
Five alert queries over the **last 24 hours**: warehouse queueing, disk spill, slow queries,
planning-time regression and failure rate. Create one Databricks SQL alert per query and schedule it
**daily** (for example 07:00): Max of the value column > the threshold in the notebook, empty result = OK.
The last cell shows your own daily p50 / p90 so you can tune the thresholds. Requires
`system.query.history` and `system.compute.warehouses`.

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
