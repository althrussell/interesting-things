-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Column-Mask Performance Diagnostic
-- MAGIC
-- MAGIC **Goal.** Find the **tables and mask functions that actually hurt query performance** — not just what
-- MAGIC is masked. It attributes real query time (from `system.query.history` + lineage) to tables and to the
-- MAGIC mask functions applied on them, flags the expensive kind (Python / non-deterministic), surfaces the
-- MAGIC slowest queries with their bottleneck signals, and reads a query plan to prove whether masking is in
-- MAGIC the critical path.
-- MAGIC
-- MAGIC **100% read-only.** Every cell is a `SELECT`/`EXPLAIN` against `system.*` and `information_schema`.
-- MAGIC It **creates nothing** and is safe in production.
-- MAGIC
-- MAGIC **Requires** the `system.query`, `system.access` (lineage), and `system.information_schema` system
-- MAGIC schemas to be enabled (standard on Unity Catalog).
-- MAGIC
-- MAGIC **Parameters (edit inline):** each cell uses `INTERVAL 3 DAYS` for the look-back and
-- MAGIC `execution_duration_ms > 2000` (2s) as the "slow" threshold. Widen/narrow to taste. To focus on one
-- MAGIC catalog, add `WHERE source_table_full_name LIKE 'your_catalog.%'`.
-- MAGIC
-- MAGIC **Tip:** for a large estate, run this on a **SQL warehouse** (Sections 1–2 join `query.history` to
-- MAGIC `table_lineage`, which is heavy on a small serverless notebook). Start at 3 days, then widen once it's
-- MAGIC responsive.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Tables that hurt the most
-- MAGIC Ranks tables by the total query time spent reading them (slow `SELECT`s, last 7 days), and flags which
-- MAGIC are masked. This is the "where is the time going" view. `is_masked = true` **and** high `total_exec_s`
-- MAGIC is the only combination where column masking could plausibly be implicated — confirm it in Sections 3 & 5.

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, execution_duration_ms, read_bytes
  FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
    AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
    AND from_result_cache = false AND execution_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name
  FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
masked AS (
  SELECT DISTINCT table_catalog||'.'||table_schema||'.'||table_name AS tbl
  FROM system.information_schema.column_masks
)
SELECT
  l.source_table_full_name                       AS table_name,
  (l.source_table_full_name IN (SELECT tbl FROM masked)) AS is_masked,
  count(*)                                        AS slow_queries,
  round(sum(s.execution_duration_ms)/1000.0, 1)   AS total_exec_s,
  round(avg(s.execution_duration_ms)/1000.0, 1)   AS avg_exec_s,
  round(sum(s.read_bytes)/1e9, 1)                 AS total_read_gb
FROM slow s
JOIN lin l USING (statement_id)
GROUP BY 1, 2
ORDER BY total_exec_s DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Mask functions that hurt the most
-- MAGIC Attributes that same slow-query time to the **mask function** applied on each hot table, and shows the
-- MAGIC function's language. This answers "which masking functions sit on the heaviest workloads." (It measures
-- MAGIC *exposure* — a function on a hot table — not proof the function itself is the cost; Sections 3 & 5 test that.)

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, execution_duration_ms
  FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
    AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
    AND from_result_cache = false AND execution_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name
  FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
tbl_cost AS (
  SELECT l.source_table_full_name AS tbl, sum(s.execution_duration_ms) AS exec_ms, count(*) AS q
  FROM slow s JOIN lin l USING (statement_id) GROUP BY 1
),
mask AS (
  SELECT table_catalog||'.'||table_schema||'.'||table_name AS tbl, mask_name
  FROM system.information_schema.column_masks
)
SELECT
  m.mask_name                                     AS mask_function,
  coalesce(r.external_language, 'SQL or built-in') AS language,
  r.is_deterministic,
  count(DISTINCT m.tbl)                           AS tables_masked,
  round(sum(c.exec_ms)/1000.0, 1)                 AS hot_table_exec_s,
  sum(c.q)                                        AS slow_queries_on_those_tables
FROM mask m
JOIN tbl_cost c USING (tbl)
LEFT JOIN system.information_schema.routines r
  ON concat(r.routine_catalog, '.', r.routine_schema, '.', r.routine_name) = m.mask_name
GROUP BY 1, 2, 3
ORDER BY hot_table_exec_s DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. The expensive kind: Python and/or non-deterministic mask functions
-- MAGIC Python UDF masks are ~17× slower than SQL and force queries **off Photon**; non-deterministic functions
-- MAGIC block optimizations. **If this returns no rows, none of your masks are the slow kind** — masking is not
-- MAGIC your performance problem, and the hot tables in Section 1 are slow for other reasons (see Section 4).

-- COMMAND ----------

SELECT DISTINCT
  r.routine_catalog, r.routine_schema, r.routine_name,
  r.external_language, r.is_deterministic
FROM system.information_schema.routines r
JOIN (SELECT DISTINCT mask_name AS fn FROM system.information_schema.column_masks) m
  ON concat(r.routine_catalog, '.', r.routine_schema, '.', r.routine_name) = m.fn
WHERE (r.external_language = 'Python' OR lower(r.is_deterministic) IN ('no', 'false'))
  AND r.routine_catalog <> 'system'   -- exclude Databricks' own built-in system masks (not yours to change)
ORDER BY r.external_language DESC NULLS LAST, r.is_deterministic;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Slowest queries — where is the time actually going?
-- MAGIC The bottleneck signals for the heaviest queries. Look for **spill** (memory pressure), **shuffle**
-- MAGIC (joins/aggregations), low **cache_pct** (cold reads), and low **pruned_files** (weak
-- MAGIC partitioning/clustering). These — not SQL masks — are the usual causes. (`statement_preview` may show
-- MAGIC `<REDACTED>` if your own governance masks the query-text column.)

-- COMMAND ----------

SELECT
  round(execution_duration_ms/1000.0, 1)   AS exec_s,
  round(compilation_duration_ms/1000.0, 1) AS compile_s,
  round(read_bytes/1e9, 2)                 AS read_gb,
  read_rows,
  round(spilled_local_bytes/1e9, 2)        AS spill_gb,
  round(shuffle_read_bytes/1e9, 2)         AS shuffle_gb,
  read_io_cache_percent                    AS cache_pct,
  read_files, pruned_files,
  executed_by,
  left(replace(statement_text, '\n', ' '), 90) AS statement_preview
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
  AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
  AND from_result_cache = false
ORDER BY execution_duration_ms DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Read the plan — is masking in the critical path?
-- MAGIC `EXPLAIN FORMATTED` shows exactly how a mask is applied. **What to look for:**
-- MAGIC
-- MAGIC - **`PhotonSecureView` / "fully supported by Photon"** → a SQL/built-in mask, applied natively and cheaply. Masking is **not** the bottleneck.
-- MAGIC - **`BatchEvalPython` / `PythonUDF`** → a **Python** mask. This runs row-by-row, is ~17× slower, and drops the query off Photon. This is a real cost — rewrite as SQL.
-- MAGIC - **`missing`/`partial` statistics** in the plan → run `ANALYZE TABLE … COMPUTE STATISTICS` (a common real cause of slowness).
-- MAGIC
-- MAGIC The example below runs on `system.access.audit` (present in every workspace). **Swap in your own top
-- MAGIC masked table + column from Section 1** to inspect it.

-- COMMAND ----------

EXPLAIN FORMATTED
SELECT request_params
FROM system.access.audit
WHERE event_date > current_date() - INTERVAL 1 DAYS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Mask density — worst case for `SELECT *`
-- MAGIC Even cheap SQL masks add up if a query selects many masked columns. These tables are the worst case for
-- MAGIC `SELECT *`; prefer explicit column lists that avoid masked columns you don't need.

-- COMMAND ----------

WITH total AS (
  SELECT table_catalog, table_schema, table_name, count(*) AS total_columns
  FROM system.information_schema.columns GROUP BY 1, 2, 3
),
masked AS (
  SELECT table_catalog, table_schema, table_name, count(*) AS masked_columns
  FROM system.information_schema.column_masks GROUP BY 1, 2, 3
)
SELECT
  m.table_catalog, m.table_schema, m.table_name,
  m.masked_columns, t.total_columns,
  round(100.0 * m.masked_columns / nullif(t.total_columns, 0), 1) AS pct_masked
FROM masked m LEFT JOIN total t USING (table_catalog, table_schema, table_name)
ORDER BY m.masked_columns DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Maintenance & Predictive Optimization — are OPTIMIZE / VACUUM / ANALYZE running?
-- MAGIC Stale or missing table maintenance is one of the most common causes of slow queries: no `OPTIMIZE`/liquid
-- MAGIC clustering means small files and poor pruning, and stale `ANALYZE` means the optimizer plans blind.
-- MAGIC [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) runs
-- MAGIC these for you on managed tables. **This first query shows whether it is running at all** (last 30 days).
-- MAGIC If it returns no rows, no automatic maintenance is happening anywhere.

-- COMMAND ----------

SELECT
  operation_type,
  count(*)                                                            AS operations,
  count(DISTINCT concat(catalog_name,'.',schema_name,'.',table_name)) AS tables,
  date(max(end_time))                                                 AS most_recent
FROM system.storage.predictive_optimization_operations_history
WHERE end_time > current_timestamp() - INTERVAL 30 DAYS
GROUP BY operation_type
ORDER BY operations DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Are your hottest tables actually being maintained?
-- MAGIC Joins your hot tables (as in Section 1) to their last `OPTIMIZE`/`VACUUM`/`ANALYZE`. A blank date or
-- MAGIC **`NOT maintained by PO`** on a hot table — or a `last_analyze` months behind `last_optimize` — is a
-- MAGIC direct "you are not doing what you need" signal. (Databricks-managed `system.*` tables are expected to
-- MAGIC show as not-PO-maintained; focus on your own catalogs. Manually-run maintenance appears in Section 7c.)

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, execution_duration_ms FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS AND statement_type = 'SELECT'
    AND execution_status = 'FINISHED' AND from_result_cache = false AND execution_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
hot AS (
  SELECT l.source_table_full_name AS tbl, count(*) AS slow_queries,
         round(sum(s.execution_duration_ms)/1000.0, 1) AS total_exec_s
  FROM slow s JOIN lin l USING (statement_id) GROUP BY 1
),
maint AS (
  SELECT lower(concat(catalog_name,'.',schema_name,'.',table_name)) AS tbl,
         max(CASE WHEN operation_type IN ('COMPACTION','CLUSTERING') THEN end_time END) AS last_optimize,
         max(CASE WHEN operation_type = 'VACUUM' THEN end_time END)                     AS last_vacuum,
         max(CASE WHEN operation_type = 'ANALYZE' THEN end_time END)                    AS last_analyze
  FROM system.storage.predictive_optimization_operations_history
  WHERE operation_status = 'SUCCESSFUL' GROUP BY 1
)
SELECT h.tbl, h.slow_queries, h.total_exec_s,
       date(m.last_optimize) AS last_optimize,
       date(m.last_vacuum)   AS last_vacuum,
       date(m.last_analyze)  AS last_analyze,
       CASE WHEN m.tbl IS NULL THEN 'NOT maintained by PO' ELSE 'maintained' END AS maintenance
FROM hot h LEFT JOIN maint m ON lower(h.tbl) = m.tbl
ORDER BY h.total_exec_s DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7c. Manual maintenance (if you don't use Predictive Optimization)
-- MAGIC If Section 7's PO history is sparse, check whether maintenance is being run by hand. Nothing here **and**
-- MAGIC nothing in the PO history means the table is not being maintained at all.

-- COMMAND ----------

SELECT
  regexp_extract(statement_text, '(?i)^\\s*(optimize|vacuum|analyze)', 1) AS operation,
  count(*)              AS runs,
  date(max(start_time)) AS most_recent
FROM system.query.history
WHERE (statement_text ILIKE 'OPTIMIZE %' OR statement_text ILIKE 'VACUUM %' OR statement_text ILIKE 'ANALYZE %')
  AND start_time > current_timestamp() - INTERVAL 30 DAYS
GROUP BY 1
ORDER BY runs DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How to read your results
-- MAGIC
-- MAGIC 1. **Section 3 returns no rows** (no Python / non-deterministic masks) → column masking is **not** your
-- MAGIC    bottleneck. Your masks are SQL/built-in, applied as Photon-native secure views (Section 5). Focus the
-- MAGIC    investigation on the hot tables in Section 1 using the Section 4 signals: **spill** → bigger warehouse
-- MAGIC    / less shuffle; **cold cache / low pruning** → `OPTIMIZE`, liquid clustering, better partitioning,
-- MAGIC    `ANALYZE … COMPUTE STATISTICS`; heavy **shuffle** → join/aggregation tuning.
-- MAGIC 2. **Section 3 returns Python or non-deterministic masks** → cross-check them against the hot tables in
-- MAGIC    Sections 1–2. Where a Python mask sits on a hot table, that is a genuine cost — rewrite it as a
-- MAGIC    deterministic SQL function:
-- MAGIC    ```sql
-- MAGIC    CREATE OR REPLACE FUNCTION mask_string(v STRING)
-- MAGIC    RETURNS STRING
-- MAGIC    RETURN CASE WHEN is_account_group_member('admins') THEN v ELSE '***' END;
-- MAGIC    ```
-- MAGIC 3. **High-density tables (Section 6)** in heavy `SELECT *` workloads → switch to explicit column lists.
