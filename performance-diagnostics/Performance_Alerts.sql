-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Daily early-warning SQL alerts for warehouse and query performance
-- MAGIC
-- MAGIC Five Databricks SQL alerts that run **once a day** over the system tables and tell the platform team about a
-- MAGIC slowdown **before** users report it. Each query looks at the **last 24 hours**, so a run at 07:00 covers 07:00
-- MAGIC yesterday to 07:00 today. Run the cells to see what each alert would see right now, then create the alerts
-- MAGIC (about 2 minutes each):
-- MAGIC
-- MAGIC 1. In the workspace sidebar, click **Alerts** → **Create alert**.
-- MAGIC 2. Paste the query from the cell below (one alert per query). Choose a SQL warehouse (a small serverless one is enough).
-- MAGIC 3. **Condition:** pick the value column, aggregation **Max**, operator **>**, and the threshold shown for that alert.
-- MAGIC    Set **Empty result state** to **OK** (no rows means nothing crossed the line).
-- MAGIC 4. **Schedule:** click the calendar icon, choose **Every 1 day** at **07:00** in your time zone (before the working
-- MAGIC    day starts). One run a day is enough: these alerts are for trends, not paging.
-- MAGIC 5. **Notify:** your platform team's email, or a Slack / Teams / webhook notification destination set up by a
-- MAGIC    workspace admin. Set the repeat notification frequency to **1 hour**: with one run a day, that re-sends at each
-- MAGIC    morning's run while the problem lasts. Turn on **Notify on OK** to hear when it clears.
-- MAGIC 6. Name it, for example `[Perf daily] Warehouse queueing`, and save.
-- MAGIC
-- MAGIC | # | Alert (last 24 hours) | Value column | Fire when Max > | Usual cause → first action |
-- MAGIC |---|---|---|---|---|
-- MAGIC | 1 | Warehouse queueing | `queue_minutes` | 30 | not enough clusters → raise max clusters |
-- MAGIC | 2 | Disk spill | `spill_gib` | 25 | not enough memory → increase size, or tune the query |
-- MAGIC | 3 | Slow queries | `slow_queries` (over 60s) | 25 | check alerts 1, 2 and 4, then query profiles |
-- MAGIC | 4 | Planning-time regression | `planning_s` | 30 | table-lookup column masks, very wide or nested views |
-- MAGIC | 5 | Failure rate | `pct_failed` (warehouses with 50+ queries) | 10 | read `error_message` in `system.query.history` |
-- MAGIC
-- MAGIC These thresholds come from a week of a production estate's history, where each alert fired on 1–4 of 16
-- MAGIC warehouses rather than on all of them. Run the last cell after a week or two and adjust them to your own baseline.
-- MAGIC Requires `SELECT` on `system.query.history` and `system.compute.warehouses`.

-- COMMAND ----------

-- MAGIC %md ### Alert 1 · Warehouse queueing (value `queue_minutes`, fire when Max > 30)

-- COMMAND ----------

WITH cfg AS (
  SELECT workspace_id, warehouse_id, warehouse_name FROM (
    SELECT workspace_id, warehouse_id, warehouse_name, delete_time,
           ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses)
  WHERE rn = 1 AND delete_time IS NULL
)
SELECT h.workspace_id,
       coalesce(c.warehouse_name, h.compute.warehouse_id) AS warehouse,
       round(sum(coalesce(h.waiting_at_capacity_duration_ms, 0)) / 60000.0, 1) AS queue_minutes,
       count(*) AS queries
FROM system.query.history h
LEFT JOIN cfg c ON c.workspace_id = h.workspace_id AND c.warehouse_id = h.compute.warehouse_id
WHERE h.start_time > current_timestamp() - INTERVAL 24 HOURS
  AND h.compute.warehouse_id IS NOT NULL
GROUP BY 1, 2
HAVING queue_minutes > 0

-- COMMAND ----------

-- MAGIC %md ### Alert 2 · Disk spill (value `spill_gib`, fire when Max > 25)

-- COMMAND ----------

WITH cfg AS (
  SELECT workspace_id, warehouse_id, warehouse_name FROM (
    SELECT workspace_id, warehouse_id, warehouse_name, delete_time,
           ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses)
  WHERE rn = 1 AND delete_time IS NULL
)
SELECT h.workspace_id,
       coalesce(c.warehouse_name, h.compute.warehouse_id) AS warehouse,
       round(sum(coalesce(h.spilled_local_bytes, 0)) / power(1024, 3), 2) AS spill_gib,
       count(*) AS queries
FROM system.query.history h
LEFT JOIN cfg c ON c.workspace_id = h.workspace_id AND c.warehouse_id = h.compute.warehouse_id
WHERE h.start_time > current_timestamp() - INTERVAL 24 HOURS
  AND h.compute.warehouse_id IS NOT NULL
GROUP BY 1, 2
HAVING spill_gib > 0

-- COMMAND ----------

-- MAGIC %md ### Alert 3 · Slow queries (value `slow_queries`, fire when Max > 25)

-- COMMAND ----------

WITH cfg AS (
  SELECT workspace_id, warehouse_id, warehouse_name FROM (
    SELECT workspace_id, warehouse_id, warehouse_name, delete_time,
           ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses)
  WHERE rn = 1 AND delete_time IS NULL
)
SELECT h.workspace_id,
       coalesce(c.warehouse_name, h.compute.warehouse_id) AS warehouse,
       sum(CASE WHEN h.total_duration_ms > 60000 THEN 1 ELSE 0 END) AS slow_queries,
       count(*) AS queries
FROM system.query.history h
LEFT JOIN cfg c ON c.workspace_id = h.workspace_id AND c.warehouse_id = h.compute.warehouse_id
WHERE h.start_time > current_timestamp() - INTERVAL 24 HOURS
  AND h.compute.warehouse_id IS NOT NULL
GROUP BY 1, 2
HAVING slow_queries > 0

-- COMMAND ----------

-- MAGIC %md ### Alert 4 · Planning-time regression (value `planning_s`, fire when Max > 30)

-- COMMAND ----------

SELECT executed_by,
       compute.type AS compute_type,
       round(compilation_duration_ms / 1000.0, 1) AS planning_s,
       round(execution_duration_ms / 1000.0, 1)   AS execution_s,
       left(replace(statement_text, '\n', ' '), 120) AS statement_preview,
       statement_id
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 24 HOURS
  AND compilation_duration_ms > execution_duration_ms
  AND compilation_duration_ms > 5000
ORDER BY compilation_duration_ms DESC
LIMIT 50

-- COMMAND ----------

-- MAGIC %md ### Alert 5 · Failure rate (value `pct_failed`, fire when Max > 10)

-- COMMAND ----------

WITH cfg AS (
  SELECT workspace_id, warehouse_id, warehouse_name FROM (
    SELECT workspace_id, warehouse_id, warehouse_name, delete_time,
           ROW_NUMBER() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) AS rn
    FROM system.compute.warehouses)
  WHERE rn = 1 AND delete_time IS NULL
)
SELECT h.workspace_id,
       coalesce(c.warehouse_name, h.compute.warehouse_id) AS warehouse,
       round(100.0 * avg(CASE WHEN h.execution_status = 'FAILED' THEN 1 ELSE 0 END), 1) AS pct_failed,
       count(*) AS queries
FROM system.query.history h
LEFT JOIN cfg c ON c.workspace_id = h.workspace_id AND c.warehouse_id = h.compute.warehouse_id
WHERE h.start_time > current_timestamp() - INTERVAL 24 HOURS
  AND h.compute.warehouse_id IS NOT NULL
GROUP BY 1, 2
HAVING count(*) >= 50

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Calibrate the thresholds to your estate (not an alert)
-- MAGIC Daily values per warehouse over the last 14 days. A good threshold sits around the p90 here: it fires on the
-- MAGIC warehouses that are struggling, not every day on all of them.

-- COMMAND ----------

WITH d AS (
  SELECT date(start_time) AS day, workspace_id, compute.warehouse_id AS warehouse_id,
         sum(coalesce(waiting_at_capacity_duration_ms, 0)) / 60000.0           AS queue_minutes,
         sum(coalesce(spilled_local_bytes, 0)) / power(1024, 3)               AS spill_gib,
         sum(CASE WHEN total_duration_ms > 60000 THEN 1 ELSE 0 END)           AS slow_queries,
         100.0 * avg(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS pct_failed,
         count(*)                                                              AS queries
  FROM system.query.history
  WHERE start_time >= date_sub(current_date(), 14) AND start_time < current_date()
    AND compute.warehouse_id IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT 'queue_minutes' AS value_column, round(percentile_approx(queue_minutes, 0.5), 1) AS p50,
       round(percentile_approx(queue_minutes, 0.9), 1) AS p90, round(max(queue_minutes), 1) AS max,
       count(*) AS warehouse_days FROM d
UNION ALL
SELECT 'spill_gib', round(percentile_approx(spill_gib, 0.5), 1), round(percentile_approx(spill_gib, 0.9), 1),
       round(max(spill_gib), 1), count(*) FROM d
UNION ALL
SELECT 'slow_queries', percentile_approx(slow_queries, 0.5), percentile_approx(slow_queries, 0.9),
       max(slow_queries), count(*) FROM d
UNION ALL
SELECT 'pct_failed', round(percentile_approx(pct_failed, 0.5), 1), round(percentile_approx(pct_failed, 0.9), 1),
       round(max(pct_failed), 1), count(*) FROM d WHERE queries >= 50
