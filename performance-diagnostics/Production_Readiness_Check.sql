-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Production Readiness Check for a Lakehouse data product (SQL)
-- MAGIC
-- MAGIC Run this **before a new data product goes live** (for example a new "360" model), and again 2–4 weeks after
-- MAGIC launch. Point it at the product's catalog and schemas. It returns **PASS / WARN / FAIL** per check, with the
-- MAGIC evidence and the fix, and an overall verdict: **READY**, **READY WITH WARNINGS** or **NOT READY**.
-- MAGIC
-- MAGIC | Layer | Checked automatically | Guided (Section 8) |
-- MAGIC |---|---|---|
-- MAGIC | Masks & row filters | table-lookup masks (planning cost), Python and non-deterministic masks, heavily-masked tables, lookup row filters, per-table masks instead of ABAC | |
-- MAGIC | Maintenance | tables with no Predictive Optimization activity in 30 days | Predictive Optimization setting on each schema |
-- MAGIC | Files & layout | partitioned tables | clustering, file sizes, VACUUM retention |
-- MAGIC | Queries | planning-bound queries, slow p95, `SELECT *` on heavily-masked tables, spill | |
-- MAGIC | Warehouses | spill, queueing, cold starts, 2X-Small, failure rate | |
-- MAGIC
-- MAGIC **Read-only** unless you set `results_table`, in which case each run's results are appended there. Run on a
-- MAGIC **SQL warehouse** after a representative test workload. Requires `system.query`, `system.access`,
-- MAGIC `system.storage`, `system.compute` and `system.information_schema`.

-- COMMAND ----------

-- MAGIC %md ## 1. Settings

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE target_catalog      STRING DEFAULT 'main';
DECLARE OR REPLACE VARIABLE target_schemas      STRING DEFAULT 'default';             -- comma-separated
DECLARE OR REPLACE VARIABLE lookback_days       INT    DEFAULT 7;
DECLARE OR REPLACE VARIABLE results_table       STRING DEFAULT '';                    -- e.g. main.ops.readiness_runs
-- thresholds
DECLARE OR REPLACE VARIABLE lookup_masks_fail   INT    DEFAULT 10;    -- lookup masks on one table that FAIL
DECLARE OR REPLACE VARIABLE lookup_s_warehouse  DOUBLE DEFAULT 0.16;  -- planning s per lookup mask, SQL warehouse
DECLARE OR REPLACE VARIABLE lookup_s_serverless DOUBLE DEFAULT 0.65;  -- planning s per lookup mask, serverless
DECLARE OR REPLACE VARIABLE max_masked_cols     INT    DEFAULT 25;
DECLARE OR REPLACE VARIABLE planning_fail_s     DOUBLE DEFAULT 5;
DECLARE OR REPLACE VARIABLE p95_warn_s          DOUBLE DEFAULT 10;
DECLARE OR REPLACE VARIABLE spill_gib           DOUBLE DEFAULT 1;
DECLARE OR REPLACE VARIABLE queue_min           DOUBLE DEFAULT 10;
DECLARE OR REPLACE VARIABLE coldstart_min       DOUBLE DEFAULT 5;

-- COMMAND ----------

-- MAGIC %md ## 2. Inventory

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW rc_tables AS
SELECT table_catalog, table_schema, table_name, table_type, created,
       lower(concat_ws('.', table_catalog, table_schema, table_name)) AS fqn
FROM system.information_schema.tables
WHERE table_catalog = target_catalog
  AND array_contains(split(replace(target_schemas, ' ', ''), ','), table_schema);

SELECT table_type, count(*) AS objects FROM rc_tables GROUP BY 1 ORDER BY 2 DESC;

-- COMMAND ----------

-- MAGIC %md ## 3. Masks & row filters

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW rc_masks AS
SELECT lower(concat_ws('.', cm.table_catalog, cm.table_schema, cm.table_name)) AS fqn,
       count(*)                                                                      AS masked_cols,
       sum(CASE WHEN r.sql_data_access = 'READS_SQL_DATA' THEN 1 ELSE 0 END)         AS lookup_masks,
       sum(CASE WHEN r.external_language = 'Python' THEN 1 ELSE 0 END)               AS python_masks,
       sum(CASE WHEN lower(r.is_deterministic) IN ('no', 'false') THEN 1 ELSE 0 END) AS nondet_masks,
       sum(CASE WHEN r.routine_name IS NULL THEN 1 ELSE 0 END)                       AS unknown_masks
FROM system.information_schema.column_masks cm
LEFT JOIN system.information_schema.routines r
  ON concat_ws('.', r.routine_catalog, r.routine_schema, r.routine_name) = cm.mask_name
WHERE cm.table_catalog = target_catalog
  AND array_contains(split(replace(target_schemas, ' ', ''), ','), cm.table_schema)
GROUP BY 1;

CREATE OR REPLACE TEMPORARY VIEW rc_results_masks AS
SELECT 'Masks' AS layer, 'Table-lookup masks (planning cost)' AS check_name, fqn AS object,
       CASE WHEN lookup_masks >= lookup_masks_fail THEN 'FAIL' WHEN lookup_masks > 0 THEN 'WARN' ELSE 'PASS' END AS status,
       CASE WHEN lookup_masks > 0 THEN
         lookup_masks || ' of ' || masked_cols || ' masks read a table: up to ~'
         || round(lookup_masks * lookup_s_warehouse, 1) || 's (SQL warehouse) / ~'
         || round(lookup_masks * lookup_s_serverless, 1) || 's (serverless) of planning on every query'
       ELSE masked_cols || ' masks, none read tables' END AS evidence,
       CASE WHEN lookup_masks > 0 THEN
         'Decide access without reading a table: ABAC policies with governed tags, or masks that check '
         || 'is_account_group_member() / current_user() only.' ELSE '' END AS fix
FROM rc_masks
UNION ALL
SELECT 'Masks', 'Python masks', fqn, 'FAIL', python_masks || ' Python masks',
       'Rewrite as SQL masks (Python masks are slower and unsupported on serverless).'
FROM rc_masks WHERE python_masks > 0
UNION ALL
SELECT 'Masks', 'Non-deterministic masks', fqn, 'WARN', nondet_masks || ' non-deterministic masks',
       'Recreate with DETERMINISTIC where the result depends only on the inputs.'
FROM rc_masks WHERE nondet_masks > 0
UNION ALL
SELECT 'Masks', 'Mask functions not visible', fqn, 'WARN', unknown_masks || ' masks whose function you cannot see',
       'Grant USE/EXECUTE on the mask functions to the user running this check.'
FROM rc_masks WHERE unknown_masks > 0
UNION ALL
SELECT 'Masks', 'Heavily-masked table', fqn, 'WARN', masked_cols || ' masked columns',
       'Confirm every masked column needs masking; keep consumer queries to explicit column lists.'
FROM rc_masks WHERE masked_cols > max_masked_cols
UNION ALL
SELECT 'Masks', 'Per-table masks instead of ABAC', fqn, 'WARN', masked_cols || ' masks bound table by table',
       'For new data products, tag columns with governed tags and protect them with catalog/schema ABAC policies.'
FROM rc_masks
UNION ALL
SELECT 'Masks', 'Table-lookup row filter', lower(concat_ws('.', f.table_catalog, f.table_schema, f.table_name)),
       'WARN', 'row filter ' || f.filter_name || ' reads a table',
       'Decide rows with is_account_group_member() / current_user(), or an ABAC row filter policy.'
FROM system.information_schema.row_filters f
JOIN system.information_schema.routines r
  ON concat_ws('.', r.routine_catalog, r.routine_schema, r.routine_name) = f.filter_name
WHERE f.table_catalog = target_catalog
  AND array_contains(split(replace(target_schemas, ' ', ''), ','), f.table_schema)
  AND r.sql_data_access = 'READS_SQL_DATA';

SELECT * FROM rc_results_masks ORDER BY status, object;

-- COMMAND ----------

-- MAGIC %md ## 4. Maintenance and partitioning

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW rc_results_maint AS
WITH po AS (
  SELECT lower(concat_ws('.', catalog_name, schema_name, table_name)) AS fqn, count(*) AS ops, max(end_time) AS last_op
  FROM system.storage.predictive_optimization_operations_history
  WHERE end_time > current_timestamp() - INTERVAL 30 DAYS AND operation_status = 'SUCCESSFUL'
    AND catalog_name = target_catalog
  GROUP BY 1
)
SELECT 'Maintenance' AS layer, 'Table maintained' AS check_name, t.fqn AS object,
       CASE WHEN po.fqn IS NULL THEN 'WARN' ELSE 'PASS' END AS status,
       CASE WHEN po.fqn IS NULL THEN 'no Predictive Optimization operations in 30 days'
            ELSE po.ops || ' Predictive Optimization operations, latest ' || date(po.last_op) END AS evidence,
       CASE WHEN po.fqn IS NULL THEN
         'Confirm Predictive Optimization is enabled for the schema (Section 8), or that a job runs OPTIMIZE/ANALYZE.'
       ELSE '' END AS fix
FROM rc_tables t LEFT JOIN po USING (fqn)
WHERE t.table_type = 'MANAGED' AND t.created < current_timestamp() - INTERVAL 7 DAYS
UNION ALL
SELECT 'Files & layout', 'Partitioned table', lower(concat_ws('.', table_catalog, table_schema, table_name)), 'WARN',
       'partitioned by ' || array_join(collect_list(column_name), ', '),
       'Partition only tables over 1 TB with at least 1 GB per partition. Otherwise convert: ALTER TABLE <t> '
       || 'REPLACE PARTITIONED BY WITH CLUSTER BY AUTO;  then OPTIMIZE <t>;'
FROM system.information_schema.columns
WHERE table_catalog = target_catalog
  AND array_contains(split(replace(target_schemas, ' ', ''), ','), table_schema)
  AND partition_index IS NOT NULL
GROUP BY table_catalog, table_schema, table_name
UNION ALL
SELECT 'Files & layout', 'External table', fqn, 'WARN', 'external tables are not maintained by Predictive Optimization',
       'Prefer Unity Catalog managed tables for production data products.'
FROM rc_tables WHERE table_type = 'EXTERNAL';

SELECT * FROM rc_results_maint ORDER BY status, object;

-- COMMAND ----------

-- MAGIC %md ## 5. Queries against this data product

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW rc_query_stats AS
WITH lin AS (
  SELECT DISTINCT statement_id, lower(source_table_full_name) AS fqn
  FROM system.access.table_lineage
  WHERE event_date >= date_sub(current_date(), lookback_days)
    AND split(lower(source_table_full_name), '\\.')[0] = lower(target_catalog)
    AND array_contains(split(replace(lower(target_schemas), ' ', ''), ','), split(lower(source_table_full_name), '\\.')[1])
),
qh AS (
  SELECT statement_id, total_duration_ms, compilation_duration_ms, execution_duration_ms, spilled_local_bytes, statement_text
  FROM system.query.history
  WHERE start_time >= timestampadd(DAY, -lookback_days, current_timestamp())
    AND execution_status = 'FINISHED' AND from_result_cache = false AND statement_type = 'SELECT'
)
SELECT l.fqn,
       count(*)                                                      AS queries,
       percentile_approx(qh.total_duration_ms, 0.95) / 1000.0        AS p95_s,
       sum(CASE WHEN qh.compilation_duration_ms > planning_fail_s * 1000
                 AND qh.compilation_duration_ms > qh.execution_duration_ms THEN 1 ELSE 0 END) AS planning_bound,
       max(qh.compilation_duration_ms) / 1000.0                      AS max_planning_s,
       sum(CASE WHEN qh.statement_text ILIKE '%select *%' THEN 1 ELSE 0 END)          AS select_star,
       sum(CASE WHEN coalesce(qh.spilled_local_bytes, 0) > 1e9 THEN 1 ELSE 0 END)    AS spilling
FROM lin l JOIN qh USING (statement_id)
GROUP BY 1;

CREATE OR REPLACE TEMPORARY VIEW rc_results_queries AS
SELECT 'Queries' AS layer, 'Planning-bound queries' AS check_name, fqn AS object, 'FAIL' AS status,
       planning_bound || ' of ' || queries || ' queries planned longer than ' || planning_fail_s
       || 's and than they ran (max ' || round(max_planning_s, 1) || 's)' AS evidence,
       'Open one in Query History → query profile. Usually table-lookup masks or very wide/nested views.' AS fix
FROM rc_query_stats WHERE planning_bound > 0
UNION ALL
SELECT 'Queries', CASE WHEN p95_s > p95_warn_s THEN 'Slow p95' ELSE 'p95 latency' END, fqn,
       CASE WHEN p95_s > p95_warn_s THEN 'WARN' ELSE 'PASS' END,
       'p95 ' || round(p95_s, 1) || 's over ' || queries || ' queries',
       CASE WHEN p95_s > p95_warn_s THEN 'Check the query profile: planning vs spill vs shuffle vs pruning.' ELSE '' END
FROM rc_query_stats
UNION ALL
SELECT 'Queries', 'SELECT * on a heavily-masked table', q.fqn, 'WARN',
       q.select_star || ' SELECT * queries; ' || m.masked_cols || ' masked columns',
       'Use explicit column lists in BI models and reports.'
FROM rc_query_stats q JOIN rc_masks m USING (fqn)
WHERE q.select_star > 0 AND m.masked_cols >= max_masked_cols
UNION ALL
SELECT 'Queries', 'Disk spill', fqn, 'WARN', spilling || ' queries spilled more than 1 GB',
       'Upsize the warehouse that runs them, or tune the join/aggregation.'
FROM rc_query_stats WHERE spilling > 0
UNION ALL
SELECT 'Queries', 'Query history', target_catalog || '.' || target_schemas, 'INFO',
       'no queries in the last ' || lookback_days || ' days', 'Re-run after a representative test workload.'
WHERE NOT EXISTS (SELECT 1 FROM rc_query_stats);

SELECT * FROM rc_results_queries ORDER BY status, object;

-- COMMAND ----------

-- MAGIC %md ## 6. Warehouses that query this data product

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW rc_results_warehouses AS
WITH lin AS (
  SELECT DISTINCT statement_id FROM system.access.table_lineage
  WHERE event_date >= date_sub(current_date(), lookback_days)
    AND split(lower(source_table_full_name), '\\.')[0] = lower(target_catalog)
    AND array_contains(split(replace(lower(target_schemas), ' ', ''), ','), split(lower(source_table_full_name), '\\.')[1])
),
cfg AS (
  SELECT * FROM (
    SELECT workspace_id, warehouse_id, warehouse_name, warehouse_type, upper(warehouse_size) AS size, max_clusters,
           delete_time, ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses)
  WHERE rn = 1 AND delete_time IS NULL
),
w AS (
  SELECT coalesce(c.warehouse_name, h.compute.warehouse_id) AS warehouse, c.warehouse_type, c.size, c.max_clusters,
         count(*) AS queries,
         sum(coalesce(h.spilled_local_bytes, 0)) / power(1024, 3)           AS spill_g,
         sum(coalesce(h.waiting_at_capacity_duration_ms, 0)) / 60000.0      AS queue_m,
         sum(coalesce(h.waiting_for_compute_duration_ms, 0)) / 60000.0      AS cold_m,
         100.0 * avg(CASE WHEN h.execution_status = 'FAILED' THEN 1 ELSE 0 END) AS pct_failed
  FROM system.query.history h JOIN lin USING (statement_id)
  LEFT JOIN cfg c ON c.workspace_id = h.workspace_id AND c.warehouse_id = h.compute.warehouse_id
  WHERE h.start_time >= timestampadd(DAY, -lookback_days, current_timestamp()) AND h.compute.warehouse_id IS NOT NULL
  GROUP BY 1, 2, 3, 4
)
SELECT 'Warehouses' AS layer, 'Disk spill' AS check_name, warehouse AS object, 'WARN' AS status,
       round(spill_g, 1) || ' GiB spilled, size ' || size AS evidence,
       'Increase the warehouse size (more memory per query).' AS fix
FROM w WHERE spill_g >= spill_gib
UNION ALL
SELECT 'Warehouses', 'Capacity queueing', warehouse, 'WARN', round(queue_m, 1) || ' min queued, max clusters ' || max_clusters,
       'Raise max clusters so it can scale out for concurrency.' FROM w WHERE queue_m >= queue_min
UNION ALL
SELECT 'Warehouses', 'Cold starts', warehouse, 'WARN', round(cold_m, 1) || ' min waiting for start-up',
       'Use a serverless SQL warehouse, or lengthen auto-stop during business hours.'
FROM w WHERE cold_m >= coldstart_min AND upper(coalesce(warehouse_type, '')) <> 'SERVERLESS'
UNION ALL
SELECT 'Warehouses', 'Undersized for production', warehouse, 'WARN', '2X-Small',
       'Size production warehouses from a load test; keep 2X-Small for development.' FROM w WHERE size = '2X_SMALL'
UNION ALL
SELECT 'Warehouses', 'Failure rate', warehouse, 'WARN', round(pct_failed, 1) || '% of queries failed',
       'Review error_message for this warehouse in system.query.history.' FROM w WHERE pct_failed >= 5
UNION ALL
SELECT 'Warehouses', 'Warehouse health', warehouse, 'PASS', queries || ' queries, size ' || size, ''
FROM w WHERE spill_g < spill_gib AND queue_m < queue_min AND size <> '2X_SMALL' AND pct_failed < 5
  AND NOT (cold_m >= coldstart_min AND upper(coalesce(warehouse_type, '')) <> 'SERVERLESS');

SELECT * FROM rc_results_warehouses ORDER BY status, object;

-- COMMAND ----------

-- MAGIC %md ## 7. Verdict

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW readiness_results AS
SELECT current_timestamp() AS run_ts, target_catalog AS catalog, target_schemas AS schemas, *
FROM (SELECT * FROM rc_results_masks UNION ALL SELECT * FROM rc_results_maint
      UNION ALL SELECT * FROM rc_results_queries UNION ALL SELECT * FROM rc_results_warehouses);

SELECT CASE WHEN sum(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'NOT READY'
            WHEN sum(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) > 0 THEN 'READY WITH WARNINGS'
            ELSE 'READY' END                                  AS verdict,
       sum(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)      AS fail,
       sum(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END)      AS warn,
       sum(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END)      AS pass
FROM readiness_results;

-- COMMAND ----------

SELECT layer, check_name, object, status, evidence, fix
FROM readiness_results
ORDER BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 WHEN 'INFO' THEN 2 ELSE 3 END, layer, object;

-- COMMAND ----------

BEGIN
  IF results_table <> '' THEN
    EXECUTE IMMEDIATE 'CREATE TABLE IF NOT EXISTS ' || results_table || ' AS SELECT * FROM readiness_results WHERE false';
    EXECUTE IMMEDIATE 'INSERT INTO ' || results_table || ' SELECT * FROM readiness_results';
    SELECT 'Results appended to ' || results_table AS status;
  ELSE
    SELECT 'Results not saved (results_table is blank).' AS status;
  END IF;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 8. Guided layout check (a few minutes per data product)
-- MAGIC SQL can't read table layout details automatically, so run the `DESCRIBE DETAIL` command listed for each table
-- MAGIC and the `DESCRIBE SCHEMA EXTENDED` command for each schema, then compare with this table:
-- MAGIC
-- MAGIC | In the output | Healthy | Fix if not |
-- MAGIC |---|---|---|
-- MAGIC | `clusteringColumns` / `clusterByAuto` | set (or table under ~1 GB) | `ALTER TABLE <t> CLUSTER BY AUTO;` then `OPTIMIZE <t>;` |
-- MAGIC | `sizeInBytes ÷ numFiles` | near 256 MB (above ~64 MB) once the table has 50+ files | `OPTIMIZE <t>;` and keep Predictive Optimization on |
-- MAGIC | `properties` → `delta.deletedFileRetentionDuration` | absent (7-day default) or ≥ 7 days | `ALTER TABLE <t> SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days');` |
-- MAGIC | `partitionColumns` | empty, unless the table is > 1 TB | `ALTER TABLE <t> REPLACE PARTITIONED BY WITH CLUSTER BY AUTO;` |
-- MAGIC | Schema: `Predictive Optimization` | `ENABLE` (or inherited `ENABLE`) | `ALTER SCHEMA <s> ENABLE PREDICTIVE OPTIMIZATION;` |

-- COMMAND ----------

SELECT 'DESCRIBE DETAIL ' || fqn || ';' AS run_this, table_type
FROM rc_tables WHERE table_type IN ('MANAGED', 'EXTERNAL')
UNION ALL
SELECT DISTINCT 'DESCRIBE SCHEMA EXTENDED ' || table_catalog || '.' || table_schema || ';', 'SCHEMA'
FROM rc_tables
ORDER BY 2, 1;
