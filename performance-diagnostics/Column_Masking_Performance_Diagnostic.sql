-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Column-Mask & Query Performance Diagnostic
-- MAGIC
-- MAGIC **Goal.** Find the **tables, mask functions and queries that actually cost time**, and say *why*:
-- MAGIC slow to **plan** (compile) or slow to **run** (execute). Masks can hurt in two very different ways:
-- MAGIC
-- MAGIC - **Execution cost**: per row, only for masked columns a query selects. Small for simple SQL masks;
-- MAGIC   large for Python masks.
-- MAGIC - **Planning cost**: paid by *every* query on the table, for *every* mask on it, whatever columns are
-- MAGIC   selected. This happens when a mask function **reads a table** (for example a permission or
-- MAGIC   governance lookup). In Databricks benchmarking, a mask that ran four permission lookups added
-- MAGIC   ~0.16s of planning per mask on a SQL warehouse and ~0.65s on serverless notebook/job compute. A table
-- MAGIC   with 52 of them planned for ~8s (warehouse) or ~34s (serverless) before a 5-row query ran. Narrow
-- MAGIC   views, column lists, `LIMIT` and the result cache don't avoid it.
-- MAGIC
-- MAGIC **100% read-only.** Every cell is a `SELECT` against `system.*` and `information_schema`. It creates
-- MAGIC nothing and is safe in production.
-- MAGIC
-- MAGIC **Requires** the `system.query`, `system.access` (lineage), `system.storage` and
-- MAGIC `system.information_schema` system schemas (standard on Unity Catalog).
-- MAGIC
-- MAGIC **Parameters (edit inline):** look-back `INTERVAL 3 DAYS`; "slow" = `total_duration_ms > 2000`.
-- MAGIC Run on a **SQL warehouse** for large estates; the history × lineage joins are heavy.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Tables that cost the most time
-- MAGIC Total time of slow queries reading each table, split into **planning** (compile) and **execution**,
-- MAGIC plus how many of the table's masks are table-lookup masks. **High `pct_planning` on a table with
-- MAGIC `lookup_masks` > 0 is the mask-planning signature** (Sections 3–4 confirm it).

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, total_duration_ms, compilation_duration_ms, execution_duration_ms, read_bytes
  FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
    AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
    AND from_result_cache = false AND total_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name
  FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
fn AS (
  SELECT concat(routine_catalog, '.', routine_schema, '.', routine_name) AS fn, sql_data_access
  FROM system.information_schema.routines
),
masks AS (
  SELECT cm.table_catalog || '.' || cm.table_schema || '.' || cm.table_name AS tbl,
         count(*) AS masked_columns,
         sum(CASE WHEN f.sql_data_access = 'READS_SQL_DATA' THEN 1 ELSE 0 END) AS lookup_masks
  FROM system.information_schema.column_masks cm
  LEFT JOIN fn f ON f.fn = cm.mask_name
  GROUP BY 1
)
SELECT
  l.source_table_full_name                                              AS table_name,
  coalesce(m.masked_columns, 0)                                         AS masked_columns,
  coalesce(m.lookup_masks, 0)                                           AS lookup_masks,
  count(*)                                                              AS slow_queries,
  round(sum(s.total_duration_ms) / 1000.0, 1)                           AS total_s,
  round(avg(s.compilation_duration_ms) / 1000.0, 1)                     AS avg_planning_s,
  round(avg(s.execution_duration_ms) / 1000.0, 1)                       AS avg_execution_s,
  round(100.0 * sum(s.compilation_duration_ms) / nullif(sum(s.total_duration_ms), 0), 0) AS pct_planning,
  round(sum(s.read_bytes) / 1e9, 1)                                     AS total_read_gb
FROM slow s
JOIN lin l USING (statement_id)
LEFT JOIN masks m ON m.tbl = l.source_table_full_name
GROUP BY 1, 2, 3
ORDER BY total_s DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Mask functions on the busiest tables
-- MAGIC Which mask functions sit on the tables with the most slow-query time, their language, and whether they
-- MAGIC **read tables** (`sql_data_access = READS_SQL_DATA`). This measures *exposure*, not proof of cost;
-- MAGIC Sections 3–4 test that.

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, total_duration_ms
  FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
    AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
    AND from_result_cache = false AND total_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name
  FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
tbl_cost AS (
  SELECT l.source_table_full_name AS tbl, sum(s.total_duration_ms) AS total_ms, count(*) AS q
  FROM slow s JOIN lin l USING (statement_id) GROUP BY 1
),
mask AS (
  SELECT table_catalog || '.' || table_schema || '.' || table_name AS tbl, mask_name
  FROM system.information_schema.column_masks
)
SELECT
  m.mask_name                                      AS mask_function,
  coalesce(r.external_language, 'SQL or built-in') AS language,
  r.sql_data_access,
  r.is_deterministic,
  count(DISTINCT m.tbl)                            AS tables_masked,
  round(sum(c.total_ms) / 1000.0, 1)               AS slow_query_s_on_those_tables,
  sum(c.q)                                         AS slow_queries_on_those_tables
FROM mask m
JOIN tbl_cost c USING (tbl)
LEFT JOIN system.information_schema.routines r
  ON concat(r.routine_catalog, '.', r.routine_schema, '.', r.routine_name) = m.mask_name
GROUP BY 1, 2, 3, 4
ORDER BY slow_query_s_on_those_tables DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. The expensive kinds of mask function
-- MAGIC Mask functions in use that are:
-- MAGIC - **Table lookup** (`READS_SQL_DATA`): add *planning* time per mask on every query against the
-- MAGIC   table (~0.16s on SQL warehouses to ~0.65s on serverless notebooks/jobs for a multi-lookup check). **This is usually the most expensive pattern on wide tables.** It covers masks that call a
-- MAGIC   helper function which reads a table.
-- MAGIC - **Python UDF**: ~17× slower to *execute* than SQL, and can't be used as a mask on serverless compute.
-- MAGIC - **Non-deterministic**: blocks some optimizer rewrites.
-- MAGIC
-- MAGIC No rows means none of your masks are these kinds. Databricks' own `system` masks are excluded.

-- COMMAND ----------

WITH used AS (
  SELECT mask_name AS fn,
         count(*) AS masked_columns,
         count(DISTINCT table_catalog || '.' || table_schema || '.' || table_name) AS tables
  FROM system.information_schema.column_masks
  GROUP BY 1
)
SELECT
  u.fn AS mask_function,
  CASE WHEN r.sql_data_access = 'READS_SQL_DATA'           THEN '1 · Table lookup (planning cost)'
       WHEN r.external_language = 'Python'                 THEN '2 · Python UDF (execution cost)'
       WHEN lower(r.is_deterministic) IN ('no', 'false')   THEN '3 · Non-deterministic'
  END AS expensive_kind,
  r.sql_data_access, r.external_language, r.is_deterministic,
  u.masked_columns, u.tables
FROM used u
JOIN system.information_schema.routines r
  ON concat(r.routine_catalog, '.', r.routine_schema, '.', r.routine_name) = u.fn
WHERE r.routine_catalog <> 'system'
  AND (r.sql_data_access = 'READS_SQL_DATA'
       OR r.external_language = 'Python'
       OR lower(r.is_deterministic) IN ('no', 'false'))
ORDER BY expensive_kind, u.masked_columns DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Planning cost of each masked table: estimated vs measured
-- MAGIC For every table with masks: how many are table-lookup masks, an **estimated** planning overhead per
-- MAGIC query (lookup masks × 0.16s on SQL warehouses / × 0.65s on serverless notebooks and jobs, from Databricks
-- MAGIC benchmarking of a four-lookup permission check; simpler lookups cost less), and the **measured** average
-- MAGIC planning time of real queries on the table over the last 3 days. **The measured column is the evidence.** `masks_unknown_type` counts masks whose function
-- MAGIC you can't see (missing privileges); it shows up as a blank, not as zero.

-- COMMAND ----------

WITH fn AS (
  SELECT concat(routine_catalog, '.', routine_schema, '.', routine_name) AS fn, sql_data_access
  FROM system.information_schema.routines
),
per_tbl AS (
  SELECT cm.table_catalog || '.' || cm.table_schema || '.' || cm.table_name AS tbl,
         count(*) AS masked_columns,
         sum(CASE WHEN f.sql_data_access = 'READS_SQL_DATA' THEN 1 ELSE 0 END) AS lookup_masks,
         sum(CASE WHEN f.fn IS NULL THEN 1 ELSE 0 END)                         AS masks_unknown_type
  FROM system.information_schema.column_masks cm
  LEFT JOIN fn f ON f.fn = cm.mask_name
  WHERE cm.table_catalog <> 'system'
  GROUP BY 1
),
q AS (
  SELECT statement_id, compilation_duration_ms, execution_duration_ms
  FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
    AND statement_type = 'SELECT' AND execution_status = 'FINISHED' AND from_result_cache = false
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name AS tbl
  FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
measured AS (
  SELECT l.tbl, count(*) AS queries,
         avg(q.compilation_duration_ms) / 1000.0 AS avg_planning_s,
         avg(q.execution_duration_ms) / 1000.0   AS avg_execution_s
  FROM q JOIN lin l USING (statement_id)
  GROUP BY 1
)
SELECT
  p.tbl                                  AS table_name,
  p.masked_columns,
  p.lookup_masks,
  p.masks_unknown_type,
  round(p.lookup_masks * 0.16, 1)        AS est_planning_s_warehouse,
  round(p.lookup_masks * 0.65, 1)        AS est_planning_s_serverless,
  m.queries                              AS queries_3d,
  round(m.avg_planning_s, 1)             AS measured_avg_planning_s,
  round(m.avg_execution_s, 1)            AS measured_avg_execution_s
FROM per_tbl p
LEFT JOIN measured m USING (tbl)
ORDER BY p.lookup_masks DESC, measured_avg_planning_s DESC NULLS LAST
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Planning-bound queries (planning time > execution time)
-- MAGIC Statements that spent more time being planned than run. Execution-time rankings miss these entirely.
-- MAGIC Cross-check the tables they read against Section 4. Statements whose text starts `FGAC for …` are the
-- MAGIC engine resolving masks and row filters for a table or view. To dig into one, open it in **Query History →
-- MAGIC See query profile** and compare *compilation* with *execution* time.

-- COMMAND ----------

SELECT
  round(compilation_duration_ms / 1000.0, 1)                                 AS planning_s,
  round(execution_duration_ms / 1000.0, 1)                                   AS execution_s,
  round(100.0 * compilation_duration_ms / nullif(total_duration_ms, 0), 0)   AS pct_planning,
  executed_by,
  client_application,
  left(replace(statement_text, '\n', ' '), 100)                              AS statement_preview,
  statement_id
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
  AND execution_status = 'FINISHED' AND from_result_cache = false
  AND compilation_duration_ms > 5000
  AND compilation_duration_ms > execution_duration_ms
ORDER BY compilation_duration_ms DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Slowest queries: where the time goes
-- MAGIC For the heaviest statements: **planning** (masks, very wide views), **spill** (memory pressure, so
-- MAGIC upsize), **shuffle** (joins and aggregations), low **cache_pct** (cold reads), low **pruned_files**
-- MAGIC (weak clustering or partitioning). `statement_preview` shows `<REDACTED>` if your own governance masks
-- MAGIC the query-text column.

-- COMMAND ----------

SELECT
  round(total_duration_ms / 1000.0, 1)       AS total_s,
  round(compilation_duration_ms / 1000.0, 1) AS planning_s,
  round(execution_duration_ms / 1000.0, 1)   AS execution_s,
  round(read_bytes / 1e9, 2)                 AS read_gb,
  read_rows,
  round(spilled_local_bytes / 1e9, 2)        AS spill_gb,
  round(shuffle_read_bytes / 1e9, 2)         AS shuffle_gb,
  read_io_cache_percent                      AS cache_pct,
  read_files, pruned_files,
  executed_by,
  left(replace(statement_text, '\n', ' '), 90) AS statement_preview
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 3 DAYS
  AND statement_type = 'SELECT' AND execution_status = 'FINISHED'
  AND from_result_cache = false
ORDER BY total_duration_ms DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Mask density: worst case for `SELECT *`
-- MAGIC Tables with the most masked columns. `SELECT *` makes every mask *execute*, so prefer explicit column
-- MAGIC lists. **This does not reduce planning cost from table-lookup masks**, which every query pays (Section 4).

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
WHERE m.table_catalog <> 'system'
ORDER BY m.masked_columns DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Maintenance & Predictive Optimization: are OPTIMIZE / VACUUM / ANALYZE running?
-- MAGIC [Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)
-- MAGIC runs these automatically on Unity Catalog managed tables. **This first query shows whether it ran at all
-- MAGIC in the last 30 days.** No rows means no automatic maintenance anywhere.

-- COMMAND ----------

SELECT
  operation_type,
  count(*)                                                            AS operations,
  count(DISTINCT table_id)                                            AS tables,
  date(max(end_time))                                                 AS most_recent
FROM system.storage.predictive_optimization_operations_history
WHERE end_time > current_timestamp() - INTERVAL 30 DAYS
  AND operation_status = 'SUCCESSFUL'
GROUP BY operation_type
ORDER BY operations DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8b. Are your busiest tables being maintained?
-- MAGIC Your busiest tables (as in Section 1) with their last automatic `OPTIMIZE` / `VACUUM` / `ANALYZE`. A
-- MAGIC blank date on a busy table of your own means Predictive Optimization isn't covering it. Check 8c before
-- MAGIC acting, in case it's maintained by a job instead. Databricks-managed `system.*` tables always show blank.

-- COMMAND ----------

WITH slow AS (
  SELECT statement_id, total_duration_ms FROM system.query.history
  WHERE start_time > current_timestamp() - INTERVAL 3 DAYS AND statement_type = 'SELECT'
    AND execution_status = 'FINISHED' AND from_result_cache = false AND total_duration_ms > 2000
),
lin AS (
  SELECT DISTINCT statement_id, source_table_full_name FROM system.access.table_lineage
  WHERE event_date > current_date() - INTERVAL 4 DAYS AND source_table_full_name IS NOT NULL
),
hot AS (
  SELECT l.source_table_full_name AS tbl, count(*) AS slow_queries,
         round(sum(s.total_duration_ms) / 1000.0, 1) AS total_s
  FROM slow s JOIN lin l USING (statement_id) GROUP BY 1
),
maint AS (
  SELECT lower(concat(catalog_name, '.', schema_name, '.', table_name)) AS tbl,
         max(CASE WHEN operation_type IN ('COMPACTION', 'CLUSTERING') THEN end_time END) AS last_optimize,
         max(CASE WHEN operation_type = 'VACUUM'  THEN end_time END)                     AS last_vacuum,
         max(CASE WHEN operation_type = 'ANALYZE' THEN end_time END)                     AS last_analyze
  FROM system.storage.predictive_optimization_operations_history
  WHERE operation_status = 'SUCCESSFUL' GROUP BY 1
)
SELECT h.tbl, h.slow_queries, h.total_s,
       date(m.last_optimize) AS last_optimize,
       date(m.last_vacuum)   AS last_vacuum,
       date(m.last_analyze)  AS last_analyze,
       CASE WHEN m.tbl IS NULL THEN 'no automatic maintenance recorded' ELSE 'maintained' END AS maintenance
FROM hot h LEFT JOIN maint m ON lower(h.tbl) = m.tbl
ORDER BY h.total_s DESC
LIMIT 30;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8c. Maintenance run by hand or by jobs
-- MAGIC `OPTIMIZE` / `VACUUM` / `ANALYZE` statements run outside Predictive Optimization in the last 30 days.

-- COMMAND ----------

SELECT
  lower(regexp_extract(statement_text, '(?i)^\\s*(optimize|vacuum|analyze)', 1)) AS operation,
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
-- MAGIC 1. **Section 3 lists table-lookup masks** → this is your biggest lever on wide, heavily-masked tables.
-- MAGIC    Section 4 gives the expected planning cost per table. Change the masks so the "who can see this?"
-- MAGIC    decision doesn't read a table. For example, check group membership directly:
-- MAGIC    ```sql
-- MAGIC    CREATE OR REPLACE FUNCTION mask_string(v STRING)
-- MAGIC    RETURNS STRING
-- MAGIC    RETURN CASE WHEN is_account_group_member('pii_readers') THEN v ELSE '***' END;
-- MAGIC    ```
-- MAGIC    If your rules live in a configuration table, generate masks like this *from* the table at deploy time
-- MAGIC    instead of reading it at query time. In Databricks benchmarking this took a 52-mask table from ~34s
-- MAGIC    of planning to ~1s on serverless compute. **Narrow views, explicit column lists, `LIMIT` and the
-- MAGIC    result cache don't reduce planning cost.**
-- MAGIC 2. **Section 3 lists Python masks** → rewrite as SQL (Python masks also can't run on serverless).
-- MAGIC    **Non-deterministic masks** → mark `DETERMINISTIC` where the output depends only on the inputs.
-- MAGIC 3. **Section 5 shows planning-bound queries on tables *without* lookup masks** → look for very wide
-- MAGIC    or deeply nested views, and open the query profile.
-- MAGIC 4. **Section 6 shows execution-heavy queries** → **spill**: upsize the warehouse; **shuffle**: tune
-- MAGIC    joins and aggregations; **low pruning**: liquid clustering on filter/join columns; **cold cache**:
-- MAGIC    expected on first reads.
-- MAGIC 5. **Section 7 high-density tables in `SELECT *` workloads** → explicit column lists (execution cost).
-- MAGIC 6. **Section 8** → busy tables with no maintenance in 8b or 8c: enable Predictive Optimization on their
-- MAGIC    catalog or schema.
