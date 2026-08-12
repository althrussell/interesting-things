# Databricks notebook source
# MAGIC %md
# MAGIC # Data Classification — Cost Visibility
# MAGIC
# MAGIC This notebook breaks down what is driving your **Unity Catalog automatic data classification** spend.
# MAGIC
# MAGIC ### Background
# MAGIC Data Classification runs on **serverless compute** and is billed under the product code
# MAGIC `DATA_CLASSIFICATION` in `system.billing.usage`. It scans more than just brand-new tables:
# MAGIC
# MAGIC | Cost driver | What it is |
# MAGIC |---|---|
# MAGIC | **Incremental scans** | New / changed tables, typically within ~24h. This is the "steady-state" drip. |
# MAGIC | **Initial scans** | When a catalog/schema is *newly enabled*, the first pass scans **everything** in it. Much more costly than incremental. |
# MAGIC | **Full rescans** | A manual/triggered rescan re-evaluates **every table** in the enabled schemas — a full sweep, not just new tables. |
# MAGIC | **Churny tables** | Pipelines that **overwrite or drop/recreate** tables (e.g. full-refresh ETL) look like "changed data" and get **re-scanned every run** — a static-looking catalog can generate constant reclassification. |
# MAGIC
# MAGIC A flat daily baseline = normal incremental scanning. Spikes = initial/full rescans. A high, steady baseline usually = churny tables being rewritten repeatedly.
# MAGIC
# MAGIC ### How to use
# MAGIC 1. Attach to a **serverless SQL warehouse** (or any warehouse/cluster with access to the `system` catalog).
# MAGIC 2. Set the widgets at the top (lookback window, your discount, optional catalog filter) and click **Run all**.
# MAGIC
# MAGIC ### Access needed (a metastore admin grants these)
# MAGIC | Grant | Enables |
# MAGIC |---|---|
# MAGIC | `SELECT` on `system.billing` | Cost queries (cells 1–5). Required. |
# MAGIC | `SELECT` on `system.access` | Workspace **names** instead of just IDs. |
# MAGIC | `SELECT` on `system.data_classification` | Catalog **names**, plus the table-level scan detail in cells 6–7. |
# MAGIC
# MAGIC The name/table-level cells degrade gracefully — if a grant is missing they'll note it and the cost analysis still runs.
# MAGIC
# MAGIC ### A note on granularity
# MAGIC Databricks attributes classification **cost** down to the **catalog** level only — the billing records carry a
# MAGIC `catalog_id` but no schema or table id. So dollar figures stop at per-catalog. To see *which tables* are being
# MAGIC scanned (the activity behind the cost), cells 6–7 use `system.data_classification`, which **is** table-level.
# MAGIC
# MAGIC > **Note on pricing:** dollar figures below are *estimates* from published list prices in `system.billing.list_prices`, multiplied by the discount rate you enter. They will not exactly match your invoice but are directionally accurate for finding the cost driver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC Run this cell once to create the widgets, then adjust them at the top of the notebook.

# COMMAND ----------

dbutils.widgets.text("lookback_days", "90", "Lookback window (days)")
dbutils.widgets.text("discount_rate", "1.0", "Price multiplier (1.0 = list price, 0.85 = 15% discount)")
dbutils.widgets.text("catalog_filter", "", "Optional: filter to one catalog_id (blank = all)")

print("Lookback (days):", dbutils.widgets.get("lookback_days"))
print("Discount rate  :", dbutils.widgets.get("discount_rate"))
print("Catalog filter :", dbutils.widgets.get("catalog_filter") or "(all)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly summary
# MAGIC The headline number: DBUs and estimated $ per month. Compare against the ~$500–1000/mo you're seeing.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_trunc('MONTH', u.usage_date)                                                                    AS month,
# MAGIC   ROUND(SUM(u.usage_quantity), 1)                                                                      AS dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * lp.pricing.effective_list.default) * CAST(:discount_rate AS DOUBLE), 2) AS est_dollars
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC  AND u.usage_end_time >= lp.price_start_time
# MAGIC  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
# MAGIC WHERE u.billing_origin_product = 'DATA_CLASSIFICATION'
# MAGIC   AND u.usage_date >= DATE_SUB(CURRENT_DATE(), CAST(:lookback_days AS INT))
# MAGIC   AND (:catalog_filter = '' OR u.usage_metadata.catalog_id = :catalog_filter)
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Daily trend  (spike vs. steady-state)
# MAGIC Plot this as a **bar/line chart** with `usage_date` on the x-axis and `est_dollars` on the y-axis.
# MAGIC
# MAGIC - **Flat baseline** → normal incremental scanning of genuinely new/changed data.
# MAGIC - **Tall spikes** → an initial scan (a catalog was newly enabled) or a full rescan on that day.
# MAGIC - **High flat baseline** → tables are being rewritten repeatedly (churny ETL) and re-scanned each time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.usage_date,
# MAGIC   ROUND(SUM(u.usage_quantity), 2)                                                                      AS dbus,
# MAGIC   ROUND(SUM(u.usage_quantity * lp.pricing.effective_list.default) * CAST(:discount_rate AS DOUBLE), 2) AS est_dollars
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN system.billing.list_prices lp
# MAGIC   ON u.sku_name = lp.sku_name
# MAGIC  AND u.usage_end_time >= lp.price_start_time
# MAGIC  AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
# MAGIC WHERE u.billing_origin_product = 'DATA_CLASSIFICATION'
# MAGIC   AND u.usage_date >= DATE_SUB(CURRENT_DATE(), CAST(:lookback_days AS INT))
# MAGIC   AND (:catalog_filter = '' OR u.usage_metadata.catalog_id = :catalog_filter)
# MAGIC GROUP BY u.usage_date
# MAGIC ORDER BY u.usage_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Biggest days  (which days blew up the bill)
# MAGIC Ranks days by consumption and shows how far above the daily average each one is. Days at 3×+ the
# MAGIC average are almost always an initial scan or a full rescan — worth correlating with when a catalog was enabled.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH daily AS (
# MAGIC   SELECT
# MAGIC     u.usage_date,
# MAGIC     SUM(u.usage_quantity)                                                                    AS dbus,
# MAGIC     SUM(u.usage_quantity * lp.pricing.effective_list.default) * CAST(:discount_rate AS DOUBLE) AS est_dollars
# MAGIC   FROM system.billing.usage u
# MAGIC   LEFT JOIN system.billing.list_prices lp
# MAGIC     ON u.sku_name = lp.sku_name
# MAGIC    AND u.usage_end_time >= lp.price_start_time
# MAGIC    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
# MAGIC   WHERE u.billing_origin_product = 'DATA_CLASSIFICATION'
# MAGIC     AND u.usage_date >= DATE_SUB(CURRENT_DATE(), CAST(:lookback_days AS INT))
# MAGIC     AND (:catalog_filter = '' OR u.usage_metadata.catalog_id = :catalog_filter)
# MAGIC   GROUP BY u.usage_date
# MAGIC )
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   ROUND(dbus, 2)                                        AS dbus,
# MAGIC   ROUND(est_dollars, 2)                                 AS est_dollars,
# MAGIC   ROUND(dbus / NULLIF(AVG(dbus) OVER (), 0), 1)         AS x_vs_daily_avg
# MAGIC FROM daily
# MAGIC ORDER BY dbus DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Attribution — which catalog / workspace / identity is driving it, **with names**
# MAGIC This is the key drill-down. Cost attribution is only available to the **catalog** level (billing carries
# MAGIC `catalog_id` but no schema/table id), so this is as granular as the *dollars* get. For table-level detail,
# MAGIC see cells 6–7.
# MAGIC
# MAGIC This cell is Python so it **always returns the cost numbers**, then layers on names only if you have the
# MAGIC extra grants:
# MAGIC - **`workspace_name`** ← `system.access.workspaces_latest`
# MAGIC - **`catalog_name`** ← `system.data_classification.results` (billing has no name, so this is the bridge)
# MAGIC - **`run_as_name`** ← the **SCIM API** (see below)
# MAGIC
# MAGIC If a grant is missing, that name column is simply skipped (with a note) and the rest still works.
# MAGIC
# MAGIC #### About `run_as`
# MAGIC Automatic classification runs as a **service principal**, so `run_as` is usually a **GUID** (the SP's
# MAGIC *application ID*), not a person. There is **no `system` table** that maps a principal ID to a name — the
# MAGIC mapping only lives in the **SCIM API**. Cell 4a below pulls the user + service-principal directories via SCIM
# MAGIC and builds a `guid → display name` lookup, which cell 4b joins in as `run_as_name`. (If `run_as` is already
# MAGIC an email — i.e. a human triggered the scan — it's passed through unchanged.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. Aggregate the cost, then resolve only the principals that appear
# MAGIC Classification runs as a **service principal**, so `run_as` is a GUID (its *application ID*). We first
# MAGIC aggregate the billing rows — that yields only a **handful of distinct `run_as` GUIDs** — then look each one
# MAGIC up individually via a **targeted SCIM filter** (`applicationId eq "<guid>"`).
# MAGIC
# MAGIC > Why targeted, not a bulk list: the automatic-classification SP is a system principal that does **not**
# MAGIC > appear in a normal paginated `ServicePrincipals` listing, but a filtered lookup finds it. (A bulk list is
# MAGIC > also slow — tens of thousands of principals.) If `run_as` is an email (a human triggered the scan), we
# MAGIC > filter `Users` by `userName` instead. Unresolved ids fall back to the raw value.

# COMMAND ----------

import requests
from pyspark.sql import functions as F

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = ctx.apiUrl().get()
token = ctx.apiToken().get()

# Numerics are validated in Python (no injection); the one string param is bound via args.
lookback = int(dbutils.widgets.get("lookback_days"))
discount = float(dbutils.widgets.get("discount_rate"))
catalog_filter = dbutils.widgets.get("catalog_filter").strip()

base = spark.sql(
    f"""
    SELECT
      u.usage_metadata.catalog_id                                                       AS catalog_id,
      u.workspace_id                                                                    AS workspace_id,
      u.identity_metadata.run_as                                                        AS run_as,
      ROUND(SUM(u.usage_quantity), 2)                                                   AS dbus,
      ROUND(SUM(u.usage_quantity * lp.pricing.effective_list.default) * {discount}, 2)  AS est_dollars,
      COUNT(DISTINCT u.usage_date)                                                       AS active_days
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
      ON u.sku_name = lp.sku_name
     AND u.usage_end_time >= lp.price_start_time
     AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
    WHERE u.billing_origin_product = 'DATA_CLASSIFICATION'
      AND u.usage_date >= DATE_SUB(CURRENT_DATE(), {lookback})
      AND (:cat = '' OR u.usage_metadata.catalog_id = :cat)
    GROUP BY 1, 2, 3
    """,
    args={"cat": catalog_filter},
)
base.cache()

# The only run_as values we need to resolve — typically just a few
run_as_ids = [r["run_as"] for r in base.select("run_as").distinct().collect() if r["run_as"]]

def _scim_filter(path, attr, value, name_field="displayName"):
    """Look up one principal by an exact SCIM filter (attr eq "value")."""
    r = requests.get(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params={"filter": f'{attr} eq "{value}"'},
        timeout=30,
    )
    r.raise_for_status()
    res = r.json().get("Resources", [])
    return res[0].get(name_field) if res else None

principal_map = {}  # run_as id -> display name
try:
    for rid in run_as_ids:
        if "@" in rid:  # a human user triggered the scan
            principal_map[rid] = _scim_filter(
                "/api/2.0/preview/scim/v2/Users", "userName", rid) or rid
        else:           # a service principal (the usual case) — match on applicationId
            principal_map[rid] = _scim_filter(
                "/api/2.0/preview/scim/v2/ServicePrincipals", "applicationId", rid) or rid
    resolved = sum(1 for k, v in principal_map.items() if v != k)
    print(f"Resolved {resolved}/{len(run_as_ids)} run_as principals via targeted SCIM lookup.")
except Exception as e:
    print("SCIM lookup failed — run_as_name will fall back to the raw id.")
    print("Needs directory read scope on your token (account/workspace admin).")
    print("Details:", str(e).splitlines()[0])

# Small lookup DataFrame (only the run_as values that appear) — broadcast for the join
principal_df = (
    spark.createDataFrame(
        [(k, v) for k, v in principal_map.items()], schema=["run_as", "run_as_name"]
    )
    if principal_map else None
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Attribution table (names resolved)

# COMMAND ----------

result = base

# Resolve run_as -> friendly name via a broadcast join (falls back to the raw id if unmatched)
if principal_df is not None:
    result = (
        result.join(F.broadcast(principal_df), "run_as", "left")
              .withColumn("run_as_name", F.coalesce(F.col("run_as_name"), F.col("run_as")))
    )

# Enrich with workspace names (needs SELECT on system.access)
try:
    ws = spark.sql("SELECT workspace_id, workspace_name FROM system.access.workspaces_latest")
    result = result.join(ws, "workspace_id", "left")
except Exception as e:
    print("workspace_name skipped — need SELECT on system.access:", str(e).splitlines()[0])

# Enrich with catalog names (needs SELECT on system.data_classification)
try:
    cats = spark.sql(
        "SELECT DISTINCT catalog_id, catalog_name FROM system.data_classification.results"
    )
    result = result.join(cats, "catalog_id", "left")
except Exception as e:
    print("catalog_name skipped — need SELECT on system.data_classification:", str(e).splitlines()[0])

# Put the friendly columns first if they resolved
front = [c for c in ["catalog_name", "catalog_id", "workspace_name", "workspace_id", "run_as_name", "run_as"] if c in result.columns]
rest = [c for c in result.columns if c not in front]
display(result.select(*front, *rest).orderBy(F.col("dbus").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Why cost stops at the catalog level (reference)
# MAGIC A quick look at the raw metadata on classification billing rows. You'll see `catalog_id` is populated but
# MAGIC `schema_id` / `table_id` are empty — which is *why* the dollar attribution above can't go below the catalog.
# MAGIC Table-level visibility comes from the `system.data_classification` tables instead (next cells).

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT usage_date, sku_name, usage_quantity,
               usage_metadata.*, identity_metadata.*
        FROM system.billing.usage
        WHERE billing_origin_product = 'DATA_CLASSIFICATION'
        ORDER BY usage_date DESC
        LIMIT 25
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Table-level scan activity  (which tables are being scanned)
# MAGIC `system.data_classification.__table_statuses` tracks classification **per table**: when it was last scanned,
# MAGIC whether the last scan succeeded, and error counts. This is the closest thing to "table-level cost" — the
# MAGIC tables scanned most recently / most often are what the spend is going toward.
# MAGIC
# MAGIC **Watch for `consecutive_error_count > 0`** — tables that error get re-attempted and can quietly burn
# MAGIC repeated scans. Sort by `last_scan_time` to see what's churning.
# MAGIC
# MAGIC > Needs `SELECT` on `system.data_classification`. Degrades gracefully if not granted.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            SELECT catalog_name, schema_name, table_name,
                   last_scan_time, last_successful_scan_time,
                   table_state, consecutive_error_count, error_code
            FROM system.data_classification.__table_statuses
            WHERE (:cat = '' OR catalog_id = :cat)
            ORDER BY last_scan_time DESC
            LIMIT 200
            """,
            args={"cat": catalog_filter},
        )
    )
except Exception as e:
    print("Could not read system.data_classification.__table_statuses.")
    print("Likely the schema isn't enabled/granted on this metastore — the cost analysis above still stands.")
    print("Details:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. What is classification actually tagging?  (per table)
# MAGIC `system.data_classification.results` is the documented, public table-level view: every classified column,
# MAGIC with first/last detection times. Rolled up per table, `last_detected` tells you which tables were
# MAGIC (re)classified most recently — a strong signal for what's driving ongoing scans (e.g. tables rewritten by
# MAGIC full-refresh pipelines keep re-detecting).
# MAGIC
# MAGIC > Needs `SELECT` on `system.data_classification`. Degrades gracefully if not granted.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            SELECT catalog_name, schema_name, table_name,
                   COUNT(*)                    AS classified_columns,
                   MIN(first_detected_time)    AS first_detected,
                   MAX(latest_detected_time)   AS last_detected
            FROM system.data_classification.results
            WHERE (:cat = '' OR catalog_id = :cat)
            GROUP BY catalog_name, schema_name, table_name
            ORDER BY last_detected DESC
            LIMIT 200
            """,
            args={"cat": catalog_filter},
        )
    )
except Exception as e:
    print("Could not read system.data_classification.results.")
    print("Likely the schema isn't enabled/granted on this metastore — the cost analysis above still stands.")
    print("Details:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Full rescan vs. incremental — inferred per catalog/day
# MAGIC **There is no field that labels a scan as "full" or "incremental"** — Databricks doesn't expose scan type in
# MAGIC any system table (`table_state` is only `MONITORED` / `SCAN_FAILED`, and billing `usage_type` is just
# MAGIC `COMPUTE_TIME`). But `__table_statuses.last_scan_time` lets you **infer** it reliably:
# MAGIC
# MAGIC - **Many tables in one catalog scanned on the *same day* (a big batch)** → an **initial or full rescan** —
# MAGIC   the whole catalog got swept at once.
# MAGIC - **A handful of tables scanned on scattered days** → normal **incremental** scanning of new/changed tables.
# MAGIC
# MAGIC The `scan_share_pct` column shows what fraction of the catalog's tables were scanned that day — a value near
# MAGIC 100% is a dead giveaway for a full sweep. Correlate the big-batch days here with the cost spikes in cells 2–3.
# MAGIC
# MAGIC > This is an **inference from scan timing**, not an official scan-type flag. Needs `SELECT` on
# MAGIC > `system.data_classification`.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            WITH per_table AS (
              SELECT catalog_id, catalog_name, table_id,
                     CAST(last_scan_time AS DATE) AS scan_day
              FROM system.data_classification.__table_statuses
              WHERE last_scan_time IS NOT NULL
                AND (:cat = '' OR catalog_id = :cat)
            ),
            cat_size AS (   -- current number of monitored tables per catalog
              SELECT catalog_id, COUNT(DISTINCT table_id) AS catalog_tables
              FROM system.data_classification.__table_statuses
              GROUP BY catalog_id
            )
            SELECT
              p.catalog_name,
              p.scan_day,
              COUNT(DISTINCT p.table_id)                                             AS tables_scanned,
              c.catalog_tables,
              ROUND(100.0 * COUNT(DISTINCT p.table_id) / NULLIF(c.catalog_tables,0), 1) AS scan_share_pct,
              CASE
                WHEN 100.0 * COUNT(DISTINCT p.table_id) / NULLIF(c.catalog_tables,0) >= 60
                     THEN 'likely FULL / initial scan'
                WHEN COUNT(DISTINCT p.table_id) >= 50
                     THEN 'large batch — possible full rescan'
                ELSE 'likely incremental'
              END                                                                    AS inferred_scan_type
            FROM per_table p
            JOIN cat_size c USING (catalog_id)
            GROUP BY p.catalog_name, p.scan_day, c.catalog_tables
            ORDER BY tables_scanned DESC
            LIMIT 200
            """,
            args={"cat": catalog_filter},
        )
    )
except Exception as e:
    print("Could not read system.data_classification.__table_statuses.")
    print("Likely the schema isn't enabled/granted on this metastore — the cost analysis above still stands.")
    print("Details:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. dbt full-reload detection  (via `system.query.history`)
# MAGIC **This is the smoking gun for the "tables are being fully reloaded" theory.** dbt stamps a JSON comment on
# MAGIC every query it runs, e.g.:
# MAGIC ```
# MAGIC /* {"app": "dbt", "dbt_version": "1.11.11", "target_name": "prod",
# MAGIC     "invocation_id": "…", "node_id": "model.my_project.gold_customers"} */
# MAGIC create or replace table `main`.`gold`.`customers` as ( … )
# MAGIC ```
# MAGIC We can therefore pull dbt's writes straight out of `system.query.history` and separate:
# MAGIC
# MAGIC | Pattern | `statement_type` | Meaning for classification cost |
# MAGIC |---|---|---|
# MAGIC | `CREATE OR REPLACE TABLE` | **`REPLACE`** | **Full reload** — table rebuilt from scratch → **re-scanned every run**. The cost driver. |
# MAGIC | `INSERT OVERWRITE` / `TRUNCATE`+load | `INSERT` / `TRUNCATE` | Also a full reload of the data. |
# MAGIC | `MERGE INTO` | `MERGE` | True **incremental** — only changed rows; cheap to reclassify. |
# MAGIC
# MAGIC The query below extracts the **dbt model name** (`node_id`) and counts how often each model issues a
# MAGIC full-reload write. Models with high `full_reload_runs` against a classification-enabled catalog are almost
# MAGIC certainly what's driving the spend — the fix is to convert them to an **incremental** materialization
# MAGIC (`merge`), or exclude their schema from classification.
# MAGIC
# MAGIC > Verified against live `system.query.history`: the reliable dbt marker is `"app": "dbt"` in `statement_text`
# MAGIC > (the connector doesn't always set `client_application` to "dbt"). Needs `SELECT` on `system.query`.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            WITH dbt AS (
              SELECT
                statement_type,
                start_time,
                statement_id,
                -- pull the dbt model id out of the JSON comment dbt prepends
                regexp_extract(statement_text, '"node_id":\\\\s*"([^"]+)"', 1) AS dbt_node_id,
                -- best-effort target table from the DDL (backtick or plain identifier)
                lower(regexp_extract(
                  statement_text,
                  '(?:create\\\\s+or\\\\s+replace\\\\s+table|insert\\\\s+overwrite(?:\\\\s+table)?|truncate\\\\s+table)\\\\s+([\\`a-z0-9_.]+)',
                  1)) AS target_table,
                CASE
                  -- only CREATE OR REPLACE *TABLE* is a reclassified reload; views/temp views aren't classified
                  WHEN statement_type = 'REPLACE'
                       AND statement_text ILIKE '%create or replace table%' THEN 'full reload (CREATE OR REPLACE TABLE)'
                  WHEN statement_type = 'TRUNCATE'               THEN 'full reload (TRUNCATE)'
                  WHEN statement_type = 'INSERT'
                       AND statement_text ILIKE '%insert overwrite%' THEN 'full reload (INSERT OVERWRITE)'
                  WHEN statement_type = 'MERGE'                  THEN 'incremental (MERGE)'
                  ELSE 'other'   -- e.g. CREATE OR REPLACE VIEW/TEMP: not classified, ignored below
                END AS write_pattern
              FROM system.query.history
              WHERE start_time >= DATE_SUB(CURRENT_DATE(), {lookback})
                AND execution_status = 'FINISHED'
                AND statement_text ILIKE '%"app": "dbt"%'
                AND statement_type IN ('REPLACE','TRUNCATE','INSERT','MERGE')
            )
            SELECT
              COALESCE(NULLIF(dbt_node_id,''), '(model n/a) ' || target_table) AS dbt_model,
              target_table,
              SUM(CASE WHEN write_pattern LIKE 'full reload%' THEN 1 ELSE 0 END) AS full_reload_runs,
              SUM(CASE WHEN write_pattern = 'incremental (MERGE)' THEN 1 ELSE 0 END) AS incremental_runs,
              MIN(write_pattern) AS example_pattern,
              MAX(start_time)    AS last_run
            FROM dbt
            WHERE write_pattern <> 'other'
            GROUP BY 1, 2
            HAVING full_reload_runs > 0
            ORDER BY full_reload_runs DESC
            LIMIT 200
            """
        )
    )
except Exception as e:
    print("Could not read system.query.history.")
    print("Needs SELECT on system.query. If dbt isn't run in this workspace (or writes elsewhere), this may be empty.")
    print("Details:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9b. Cross-check — do those full-reloaded tables sit in a classification-enabled catalog?
# MAGIC Joins the dbt full-reload targets back to the tables classification is actually scanning (`__table_statuses`).
# MAGIC A match here is the **direct confirmation**: this dbt model rebuilds this table every run, and classification
# MAGIC re-scans it every time. If `system.data_classification` isn't granted this cell is skipped.

# COMMAND ----------

try:
    display(
        spark.sql(
            f"""
            WITH dbt_full AS (
              SELECT DISTINCT
                lower(regexp_extract(
                  statement_text,
                  '(?:create\\\\s+or\\\\s+replace\\\\s+table|insert\\\\s+overwrite(?:\\\\s+table)?)\\\\s+([\\`a-z0-9_.]+)',
                  1)) AS target_table,
                regexp_extract(statement_text, '"node_id":\\\\s*"([^"]+)"', 1) AS dbt_node_id
              FROM system.query.history
              WHERE start_time >= DATE_SUB(CURRENT_DATE(), {lookback})
                AND execution_status = 'FINISHED'
                AND statement_text ILIKE '%"app": "dbt"%'
                AND (statement_type = 'REPLACE'
                     OR (statement_type = 'INSERT' AND statement_text ILIKE '%insert overwrite%'))
            ),
            reload_counts AS (
              SELECT
                lower(regexp_extract(
                  statement_text,
                  '(?:create\\\\s+or\\\\s+replace\\\\s+table|insert\\\\s+overwrite(?:\\\\s+table)?)\\\\s+([\\`a-z0-9_.]+)',
                  1)) AS target_table,
                COUNT(*) AS full_reload_runs
              FROM system.query.history
              WHERE start_time >= DATE_SUB(CURRENT_DATE(), {lookback})
                AND execution_status = 'FINISHED'
                AND statement_text ILIKE '%"app": "dbt"%'
                AND (statement_type = 'REPLACE'
                     OR (statement_type = 'INSERT' AND statement_text ILIKE '%insert overwrite%'))
              GROUP BY 1
            )
            SELECT
              t.catalog_name, t.schema_name, t.table_name,
              d.dbt_node_id                       AS dbt_model,
              rc.full_reload_runs,
              t.last_scan_time,
              t.consecutive_error_count
            FROM system.data_classification.__table_statuses t
            JOIN dbt_full d
              -- match the fully-qualified name, normalising backticks the DDL uses
              ON replace(lower(t.catalog_name||'.'||t.schema_name||'.'||t.table_name), '`','')
                 = replace(d.target_table, '`','')
            JOIN reload_counts rc
              ON rc.target_table = d.target_table
            WHERE (:cat = '' OR t.catalog_id = :cat)
            ORDER BY rc.full_reload_runs DESC
            LIMIT 200
            """,
            args={"cat": catalog_filter},
        )
    )
except Exception as e:
    print("Cross-check skipped — needs SELECT on both system.query and system.data_classification.")
    print("Details:", str(e).splitlines()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to do with the findings
# MAGIC
# MAGIC Once cells 2–3 tell you whether it's **spike-driven** (initial/full rescans) or a **high steady baseline**
# MAGIC (churny tables), cell 4 names *which catalog/principal* is responsible, cells 6–7 show *which tables* within
# MAGIC it are being scanned, and cell 8 infers whether those were **full sweeps or incremental**, you can control it:
# MAGIC
# MAGIC - **Scope by catalog** — turn classification **off** on low-value or very large catalogs
# MAGIC   (*Catalog → Details → Data classification*).
# MAGIC - **Scope by schema** — switch a catalog from *"All current and future schemas"* (the default) to
# MAGIC   **only selected schemas**, so newly created schemas aren't auto-scanned.
# MAGIC - **Investigate full-refresh pipelines** — if a catalog shows a high, flat baseline, use cell 7 to find the
# MAGIC   tables with the most recent `last_detected`, then look for ETL jobs that **overwrite / drop+recreate** those
# MAGIC   tables on a schedule; each rewrite triggers a re-scan. Switching those to merge/append (or excluding those
# MAGIC   schemas) removes the recurring cost.
# MAGIC - **Fix dbt full reloads (cells 9 / 9b)** — models that run as `create or replace table` (statement type
# MAGIC   `REPLACE`) or `insert overwrite` rebuild the whole table every `dbt run`, so classification re-scans them
# MAGIC   each time. Convert the high-`full_reload_runs` models to an **incremental** materialization
# MAGIC   (`materialized='incremental'` with a `merge` strategy) so only changed rows are written — or exclude their
# MAGIC   schema from classification. Cell 9b names the exact tables where a dbt full reload meets a scanned table.
# MAGIC - **Fix erroring tables** — in cell 6, tables with `consecutive_error_count > 0` get re-attempted and can
# MAGIC   quietly burn repeated scans. Resolve the error or exclude the table.
# MAGIC - **Watch for full rescans** — a manual full rescan re-scans everything; make sure these aren't being
# MAGIC   triggered routinely.
# MAGIC - **Governance Hub → Data** shows classification coverage across your catalogs.
# MAGIC
# MAGIC ### Keep an eye on it
# MAGIC Save cell 2 as a **Databricks SQL alert** (e.g. alert when a single day exceeds your expected daily average
# MAGIC by 3×) so a future spike gets flagged instead of showing up on next month's bill.
