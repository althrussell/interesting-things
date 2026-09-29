# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Platform Health Check
# MAGIC
# MAGIC A self-service diagnostic for Databricks platform maturity: governance, table layout and maintenance, ingestion, compute, jobs, SQL serving and cost.
# MAGIC
# MAGIC This notebook reads your system tables and some table metadata. It then produces **a single ranked list of things to fix**, from P1 (fix first) to P3, with evidence and a recommended action for each.
# MAGIC
# MAGIC **It's read-only.** It runs `SELECT`, `SHOW` and `DESCRIBE` statements only. The one exception is when you set the `output_table` widget, which writes the findings to that table.
# MAGIC
# MAGIC **How to run**
# MAGIC 1. Attach the notebook to **serverless** compute, or to a Unity Catalog-enabled cluster on DBR 15.4 LTS or later.
# MAGIC 2. Set the widgets at the top:
# MAGIC    - `workspace_ids`: `all` covers every workspace in the account. Use a comma list, e.g. `1234567890123456,6543210987654321`, or `current` for this workspace only.
# MAGIC    - `lookback_days`: default 90. This is the biggest runtime lever on large accounts.
# MAGIC    - `catalogs` / `schemas` (optional): limit the table-level checks to part of the estate. Blank means all. Use comma lists, e.g. `prod_gold,prod_silver` and `sales,finance.reporting`. A schema can be a bare name, which matches in any selected catalog, or `catalog.schema`.
# MAGIC 3. Choose **Run all**. It typically takes 5-15 minutes.
# MAGIC
# MAGIC The final section shows the ranked findings and the result of each check.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - These system schemas must be enabled: `access`, `query`, `compute`, `lakeflow`, `storage` and `billing`. A metastore admin can enable them via the API. See [Enable system tables](https://docs.databricks.com/aws/en/admin/system-tables/#enable).
# MAGIC - The user running the notebook needs `SELECT` on them.
# MAGIC - For the per-table deep dive (section 4), the user also needs `SELECT` or `MANAGE`/ownership on the tables being inspected. Tables they can't read are skipped and noted.
# MAGIC - Missing schemas or permissions don't stop the run. The affected checks are skipped and listed in the check log.
# MAGIC
# MAGIC **Scope and limits**
# MAGIC - Every system-table query is limited by date (`lookback_days`) and workspace (`workspace_ids`).
# MAGIC - `catalogs` / `schemas` apply to the table-level checks: sections 1a, 3 and 4, and 5a. The Hive metastore checks (1b, 2a) always run. Path-based reads (2b), SQL command history (5b), and ingestion, compute, jobs, SQL warehouses and billing (6-10) are workspace-level, so they use the workspace and date filters only.
# MAGIC - `system.query.history` covers SQL warehouses and serverless compute. Workloads on classic job or all-purpose clusters appear in the jobs, compute and billing checks. They don't appear in the query-level checks.
# MAGIC - `$` figures are at **list price** and exclude any discounts.
# MAGIC
# MAGIC **Priority score** = `severity (1-5) × impact (1-5)` + an effort bonus (S +3, M 0, L −3).
# MAGIC - **P1** is 16 or more
# MAGIC - **P2** is 9-15
# MAGIC - **P3** is below 9

# COMMAND ----------

dbutils.widgets.text("lookback_days", "90", "1. Lookback days")
dbutils.widgets.text("workspace_ids", "all", "2. Workspace IDs (all | current | comma list)")
dbutils.widgets.text("top_n_tables", "25", "3. Top N hot tables to inspect")
dbutils.widgets.dropdown("inspect_tables", "yes", ["yes", "no"], "4. Run per-table DESCRIBE deep dive")
dbutils.widgets.text("output_table", "", "5. Optional: save findings to catalog.schema.table")
dbutils.widgets.text("catalogs", "", "6. Optional: catalogs to include (comma list; blank = all)")
dbutils.widgets.text("schemas", "", "7. Optional: schemas to include (schema or catalog.schema; blank = all)")

# COMMAND ----------

# MAGIC %md ## Setup: parameters, helpers and system table availability

# COMMAND ----------

import datetime, json, re, traceback
import pandas as pd

LOOKBACK = int(dbutils.widgets.get("lookback_days").strip() or 90)
TOP_N = int(dbutils.widgets.get("top_n_tables").strip() or 25)
INSPECT = dbutils.widgets.get("inspect_tables") == "yes"
OUTPUT_TABLE = dbutils.widgets.get("output_table").strip()
_ws_raw = dbutils.widgets.get("workspace_ids").strip().lower()

if _ws_raw in ("", "all"):
    WS_IDS = []
elif _ws_raw == "current":
    try:
        ctx = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())
        WS_IDS = [str(ctx["attributes"]["orgId"])]
    except Exception:
        try:
            WS_IDS = [str(spark.sql("SELECT current_workspace_id()").first()[0])]
        except Exception:
            raise ValueError("Could not detect this workspace ID. Set the workspace_ids widget to 'all' or a comma list of IDs.")
else:
    WS_IDS = [w.strip() for w in _ws_raw.split(",") if re.fullmatch(r"\d+", w.strip())]


def ws(col="workspace_id"):
    """Workspace filter fragment, applied to every system table query."""
    return f"AND {col} IN ({','.join(repr(w) for w in WS_IDS)})" if WS_IDS else ""


def _names(widget):
    return [n.strip().strip("`").lower() for n in dbutils.widgets.get(widget).split(",") if n.strip().strip("`")]


CATALOGS = _names("catalogs")
SCHEMAS = [s for s in _names("schemas") if "." not in s]           # bare schema: any selected catalog
CAT_SCHEMAS = [s for s in _names("schemas") if s.count(".") == 1]  # catalog.schema: exact pair
_sql_list = lambda xs: ",".join("'" + x.replace("'", "''") + "'" for x in xs)


def uc(cat_col, sch_col):
    """Catalog/schema filter fragment for table-level queries (blank widgets = no filter)."""
    f = f" AND lower({cat_col}) IN ({_sql_list(CATALOGS)})" if CATALOGS else ""
    parts = ([f"lower({sch_col}) IN ({_sql_list(SCHEMAS)})"] if SCHEMAS else []) + \
            ([f"lower(concat({cat_col}, '.', {sch_col})) IN ({_sql_list(CAT_SCHEMAS)})"] if CAT_SCHEMAS else [])
    return f + (f" AND ({' OR '.join(parts)})" if parts else "")


UC_FILTERED = bool(CATALOGS or SCHEMAS or CAT_SCHEMAS)
UC_SCOPE = "; ".join(filter(None, [f"catalogs={','.join(CATALOGS)}" if CATALOGS else "",
                                   f"schemas={','.join(SCHEMAS + CAT_SCHEMAS)}" if SCHEMAS or CAT_SCHEMAS else ""])) or "all"


D = LOOKBACK
NODE_D = min(LOOKBACK, 30)  # node_timeline is per-minute, so keep it to 30 days
TODAY = datetime.date.today()

FINDINGS, CHECK_LOG = [], []
EFFORT_BONUS = {"S": 3, "M": 0, "L": -3}


def add_finding(category, check, obj, evidence, recommendation, severity, impact, effort="M"):
    score = severity * impact + EFFORT_BONUS.get(effort, 0)
    FINDINGS.append(dict(
        priority="P1" if score >= 16 else "P2" if score >= 9 else "P3",
        score=score, category=category, check=check, object=str(obj)[:300],
        evidence=str(evidence)[:1000], recommendation=recommendation,
        severity=severity, impact=impact, effort=effort))


def q(name, sql, show=True, limit=None):
    """Run a query and return pandas; log the outcome and never raise."""
    t0 = datetime.datetime.now()
    try:
        df = spark.sql(sql)
        if limit:
            df = df.limit(limit)
        pdf = df.toPandas()
        CHECK_LOG.append(dict(check=name, status="OK", rows=len(pdf),
                              seconds=round((datetime.datetime.now() - t0).total_seconds(), 1), error=""))
        if show:
            if len(pdf):
                display(pdf)
            else:
                print(f"{name}: no rows")
        return pdf
    except Exception as e:
        msg = str(e).split("\n")[0][:300]
        CHECK_LOG.append(dict(check=name, status="SKIPPED", rows=0,
                              seconds=round((datetime.datetime.now() - t0).total_seconds(), 1), error=msg))
        print(f"{name}: SKIPPED - {msg}")
        return pd.DataFrame()


def usd(x):
    return f"${0 if pd.isna(x) else x:,.0f}"


def fmt_list(items, n=10):
    items = [str(i) for i in items]
    return ", ".join(items[:n]) + (f" (+{len(items) - n} more)" if len(items) > n else "")


SYSTEM_TABLES = [
    "system.information_schema.tables", "system.access.table_lineage", "system.query.history",
    "system.storage.predictive_optimization_operations_history", "system.compute.clusters",
    "system.compute.node_timeline", "system.compute.warehouses", "system.lakeflow.jobs",
    "system.lakeflow.job_run_timeline", "system.lakeflow.pipelines", "system.billing.usage",
    "system.billing.list_prices"]
AVAIL = {}
for t in SYSTEM_TABLES:
    try:
        spark.sql(f"SELECT 1 FROM {t} LIMIT 1").collect()
        AVAIL[t] = "available"
    except Exception as e:
        AVAIL[t] = "NOT AVAILABLE: " + str(e).split("\n")[0][:150]
        add_finding("Platform", "System table not available", t, AVAIL[t],
                    "Enable the system schema and grant SELECT, then re-run this notebook for full coverage.", 2, 2, "S")
ok = lambda t: AVAIL.get(t) == "available"

print(f"Lookback: {D} days | Workspaces: {WS_IDS or 'all in account'} | Tables: {UC_SCOPE} | Top N tables: {TOP_N} | Deep dive: {INSPECT}")
display(pd.DataFrame([{"system_table": k, "status": v} for k, v in AVAIL.items()]))

# COMMAND ----------

# MAGIC %md ## 1. Table inventory: format, managed vs external, Hive metastore
# MAGIC `system.information_schema.tables` covers the Unity Catalog metastore attached to this workspace.

# COMMAND ----------

inv = pd.DataFrame()
if ok("system.information_schema.tables"):
    inv_f = uc("table_catalog", "table_schema")
    inv = q("1a Table inventory", f"""
        SELECT table_catalog, table_type, data_source_format, COUNT(*) AS tables
        FROM system.information_schema.tables
        WHERE table_schema <> 'information_schema'
          AND table_catalog NOT IN ('system', 'samples', '__databricks_internal') {inv_f}
        GROUP BY ALL ORDER BY tables DESC""")

if len(inv):
    base = inv[inv.table_type.isin(["MANAGED", "EXTERNAL"])]
    non_delta = base[~base.data_source_format.isin(["DELTA", "ICEBERG"])]
    if len(non_delta):
        by_fmt = non_delta.groupby("data_source_format").tables.sum().to_dict()
        n = int(non_delta.tables.sum())
        add_finding("Tables & layout", "Non-Delta tables in Unity Catalog", fmt_list(sorted(non_delta.table_catalog.unique())),
                    f"{n} tables not in Delta/Iceberg format: {by_fmt}",
                    "Convert to Delta (CONVERT TO DELTA or CTAS into a managed table). Non-Delta tables get no data skipping, "
                    "no predictive optimisation, no time travel and weaker governance.", 3, 4 if n > 50 else 3, "M")
    ext = base[(base.table_type == "EXTERNAL")]
    managed_n, ext_n = int(base[base.table_type == "MANAGED"].tables.sum()), int(ext.tables.sum())
    if ext_n and ext_n >= 0.2 * (managed_n + ext_n):
        add_finding("Governance", "High share of EXTERNAL tables", fmt_list(sorted(ext.table_catalog.unique())),
                    f"{ext_n} external vs {managed_n} managed tables ({round(100 * ext_n / (managed_n + ext_n))}% external)",
                    "Move tables to UC managed (ALTER TABLE ... SET MANAGED, or CTAS). Predictive optimisation, automatic "
                    "liquid clustering and managed-table performance features only apply to managed tables.", 3, 3, "M")

# Hive metastore: anything still registered there?
hms_rows = []
try:
    schemas = [r[0] for r in spark.sql("SHOW SCHEMAS IN hive_metastore").collect()]
    for s in schemas[:100]:
        try:
            n = spark.sql(f"SHOW TABLES IN hive_metastore.`{s}`").filter("isTemporary = false").count()
            if n:
                hms_rows.append({"hms_schema": s, "tables": n})
        except Exception:
            pass
    CHECK_LOG.append(dict(check="1b Hive metastore inventory", status="OK", rows=len(hms_rows), seconds=0, error=""))
except Exception as e:
    CHECK_LOG.append(dict(check="1b Hive metastore inventory", status="SKIPPED", rows=0, seconds=0, error=str(e).split("\n")[0][:300]))
hms = pd.DataFrame(hms_rows)
if len(hms):
    display(hms)
    add_finding("Governance", "Tables still registered in Hive metastore", fmt_list(hms.hms_schema.tolist()),
                f"{int(hms.tables.sum())} tables across {len(hms)} hive_metastore schemas",
                "Migrate to Unity Catalog (UCX or SYNC / CTAS), then disable legacy HMS access. HMS tables sit outside "
                "UC governance, lineage and predictive optimisation.", 4, 4 if hms.tables.sum() > 20 else 3, "M")
else:
    print("No Hive metastore tables found (or HMS not accessible).")

# COMMAND ----------

# MAGIC %md ## 2. Governance: Hive metastore and path-based access in the last N days

# COMMAND ----------

if ok("system.access.table_lineage"):
    hms_reads = q("2a Hive metastore reads/writes", f"""
        SELECT workspace_id, entity_type,
               COUNT(*) AS events, COUNT(DISTINCT created_by) AS users,
               COUNT(DISTINCT coalesce(source_table_full_name, target_table_full_name)) AS tables,
               MAX(event_date) AS last_seen
        FROM system.access.table_lineage
        WHERE event_date >= current_date() - {D} {ws()}
          AND (source_table_catalog = 'hive_metastore' OR target_table_catalog = 'hive_metastore')
        GROUP BY ALL ORDER BY events DESC""")
    if len(hms_reads):
        add_finding("Governance", "Workloads still using Hive metastore", fmt_list(hms_reads.entity_type.unique()),
                    f"{int(hms_reads.events.sum())} lineage events on {int(hms_reads.tables.sum())} HMS tables by "
                    f"{int(hms_reads.users.max())}+ users in {D} days",
                    "Repoint these workloads to UC tables before new use cases rely on the same data.", 4, 4, "M")

    paths = q("2b Path-based (non-table) access", f"""
        SELECT workspace_id, source_type, regexp_extract(source_path, '^([a-z0-9]+://[^/]+/[^/]+)', 1) AS storage_prefix,
               COUNT(*) AS reads, COUNT(DISTINCT created_by) AS users, COUNT(DISTINCT source_path) AS paths, MAX(event_date) AS last_seen
        FROM system.access.table_lineage
        WHERE event_date >= current_date() - {D} {ws()}
          AND source_table_full_name IS NULL AND source_path IS NOT NULL
          AND source_path NOT LIKE '/Volumes/%' AND source_path NOT LIKE 'dbfs:/Volumes/%'
        GROUP BY ALL ORDER BY reads DESC""", limit=50)
    if len(paths) and paths.reads.sum() >= 50:
        add_finding("Governance", "Direct storage-path reads (bypassing tables)", fmt_list(paths.storage_prefix.unique(), 5),
                    f"{int(paths.reads.sum())} path-based reads of {int(paths.paths.sum())} paths by up to {int(paths.users.max())} users",
                    "Register the data as UC tables or Volumes and read by name. Path access bypasses table-level "
                    "permissions and lineage.", 3, 3, "M")

# COMMAND ----------

# MAGIC %md ## 3. Hot tables: which tables drive query volume, scan cost and write churn
# MAGIC Joins `system.query.history` (SQL warehouses and serverless) to `system.access.table_lineage` on `statement_id`. Query metrics cover the whole statement, so a statement that reads several tables counts toward each of them.

# COMMAND ----------

hot_read = hot_write = pd.DataFrame()
if ok("system.query.history") and ok("system.access.table_lineage"):
    src_f, tgt_f = uc("source_table_catalog", "source_table_schema"), uc("target_table_catalog", "target_table_schema")
    lin_f = f"AND ((1=1 {src_f}) OR (1=1 {tgt_f}))" if UC_FILTERED else ""
    qh_f = "AND statement_id IN (SELECT statement_id FROM lin)" if UC_FILTERED else ""  # only statements touching in-scope tables
    base_cte = f"""
        WITH lin AS (
          SELECT DISTINCT statement_id,
                 CASE WHEN 1=1 {src_f} THEN source_table_full_name END AS source_table_full_name,
                 source_table_catalog,
                 CASE WHEN 1=1 {tgt_f} THEN target_table_full_name END AS target_table_full_name,
                 target_table_catalog
          FROM system.access.table_lineage
          WHERE event_date >= current_date() - {D} {ws()} AND statement_id IS NOT NULL
            {lin_f}),
        qh AS (
          SELECT statement_id, client_application, statement_type, read_files, pruned_files, read_bytes,
                 total_duration_ms, written_files, written_bytes
          FROM system.query.history
          WHERE start_time >= current_date() - INTERVAL {D} DAYS {ws()} AND execution_status = 'FINISHED'
            {qh_f})"""
    hot_read = q("3a Hot tables by read volume", base_cte + f"""
        SELECT l.source_table_full_name AS table_name, COUNT(DISTINCT qh.statement_id) AS queries,
               COUNT(DISTINCT qh.client_application) AS clients, concat_ws(', ', slice(collect_set(qh.client_application), 1, 4)) AS top_clients,
               ROUND(SUM(read_bytes) / power(1024, 4), 3) AS read_tb,
               ROUND(AVG(read_files)) AS avg_files_per_query,
               ROUND(SUM(read_bytes) / nullif(SUM(read_files), 0) / 1048576, 2) AS avg_mb_per_file,
               ROUND(100 * SUM(pruned_files) / nullif(SUM(pruned_files) + SUM(read_files), 0), 1) AS pruning_pct,
               ROUND(percentile(total_duration_ms, 0.95) / 1000, 1) AS p95_sec
        FROM qh JOIN (SELECT DISTINCT statement_id, source_table_full_name FROM lin
                     WHERE source_table_full_name IS NOT NULL AND source_table_catalog NOT IN ('system', 'samples')) l USING (statement_id)
        WHERE qh.statement_type = 'SELECT'
        GROUP BY 1 ORDER BY read_tb DESC, queries DESC LIMIT {TOP_N}""")
    hot_write = q("3b Hot tables by write activity", base_cte + f"""
        SELECT l.target_table_full_name AS table_name, COUNT(DISTINCT qh.statement_id) AS writes,
               concat_ws(', ', collect_set(qh.statement_type)) AS statement_types,
               SUM(CASE WHEN statement_type IN ('UPDATE', 'MERGE', 'DELETE') THEN 1 ELSE 0 END) AS dml_writes,
               SUM(written_files) AS files_written,
               ROUND(SUM(written_bytes) / nullif(SUM(written_files), 0) / 1048576, 2) AS avg_mb_per_file_written,
               ROUND(SUM(written_bytes) / power(1024, 3), 2) AS written_gb
        FROM qh JOIN (SELECT DISTINCT statement_id, target_table_full_name FROM lin
                     WHERE target_table_full_name IS NOT NULL AND target_table_catalog NOT IN ('system', 'samples')) l USING (statement_id)
        WHERE qh.written_files > 0
        GROUP BY 1 ORDER BY writes DESC LIMIT {TOP_N}""")

if len(hot_write):
    for _, r in hot_write[(hot_write.writes >= 20) & (hot_write.avg_mb_per_file_written < 8) & (hot_write.files_written >= 1000)].iterrows():
        add_finding("Tables & layout", "Write pattern creating small files", r.table_name,
                    f"{r.writes} writes ({r.statement_types}) produced {r.files_written} files at ~{r.avg_mb_per_file_written} MB/file",
                    "Avoid repeated CREATE OR REPLACE full rewrites and over-partitioning. Use MERGE/incremental writes "
                    "with optimised writes/auto compaction (on by default for UC managed tables).", 3, 3, "S")
    for _, r in hot_write[hot_write.dml_writes >= 500].iterrows():
        add_finding("Architecture", "OLTP-style row-level writes on a Delta table", r.table_name,
                    f"{r.dml_writes} UPDATE/MERGE/DELETE statements in {D} days",
                    "Batch the writes, or move the operational workload to Lakebase (Postgres) and sync it to Delta.", 3, 3, "M")

# COMMAND ----------

# MAGIC %md ## 4. Table deep dive: file layout, clustering, predictive optimisation and maintenance history
# MAGIC Runs `DESCRIBE DETAIL`, `DESCRIBE TABLE EXTENDED` and `DESCRIBE HISTORY` on the hot tables from section 3, plus the largest tables by predictive optimisation activity. Views and tables you can't access are skipped.

# COMMAND ----------

detail = pd.DataFrame()
if INSPECT:
    cands = list(dict.fromkeys(list(hot_read.get("table_name", [])) + list(hot_write.get("table_name", []))))
    n_hot = len(cands)
    if cands and ok("system.information_schema.tables"):
        # Keep only tables that still exist and support DESCRIBE DETAIL (not views)
        names = ",".join("'" + c.replace("'", "''") + "'" for c in cands)
        live = {r[0] for r in spark.sql(f"""
            SELECT concat_ws('.', table_catalog, table_schema, table_name) FROM system.information_schema.tables
            WHERE concat_ws('.', table_catalog, table_schema, table_name) IN ({names})
              AND table_type IN ('MANAGED', 'EXTERNAL', 'STREAMING_TABLE')""").collect()}
        cands = [c for c in cands if c in live]
    rows, since = [], datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=D)
    for t in cands[: TOP_N * 2]:
        fq = ".".join(f"`{p}`" for p in t.split("."))
        rec = {"table_name": t}
        try:
            d = spark.sql(f"DESCRIBE DETAIL {fq}").first().asDict()
            rec.update(format=d.get("format"), num_files=d.get("numFiles"), size_gb=round((d.get("sizeInBytes") or 0) / 1024 ** 3, 2),
                       partition_cols=",".join(d.get("partitionColumns") or []), clustering_cols=",".join(d.get("clusteringColumns") or []),
                       cluster_by_auto=d.get("clusterByAuto"))
            rec["avg_file_mb"] = round((d.get("sizeInBytes") or 0) / max(d.get("numFiles") or 1, 1) / 1024 ** 2, 2)
            ext = {r[0]: r[1] for r in spark.sql(f"DESCRIBE TABLE EXTENDED {fq}").collect() if r[0]}
            rec["table_type"] = ext.get("Type")
            rec["predictive_optimization"] = ext.get("Predictive Optimization", "n/a")
            h = spark.sql(f"DESCRIBE HISTORY {fq} LIMIT 1000").select("timestamp", "operation").toPandas()
            h = h[pd.to_datetime(h.timestamp, utc=True) >= since]
            rec["commits_in_window"] = len(h)
            rec["optimize_runs"] = int((h.operation == "OPTIMIZE").sum())
            rec["vacuum_runs"] = int(h.operation.str.startswith("VACUUM").sum())
            rec["full_rewrites"] = int(h.operation.str.contains("CREATE OR REPLACE|REPLACE TABLE", regex=True).sum())
            rec["status"] = "OK"
        except Exception as e:
            rec["status"] = "SKIPPED: " + str(e).split("\n")[0][:120]
        rows.append(rec)
    detail = pd.DataFrame(rows)
    CHECK_LOG.append(dict(check="4 Table deep dive", status="OK", rows=len(detail), seconds=0,
                          error=f"{n_hot - len(cands)} of {n_hot} hot tables not inspectable (views, dropped or no privilege); "
                                f"{(detail.status != 'OK').sum() if len(detail) else 0} skipped on DESCRIBE"))
    if len(detail):
        display(detail)

if len(detail) and "num_files" in detail:
    d_ok = detail[detail.status == "OK"]
    for _, r in d_ok[(d_ok.avg_file_mb < 32) & (d_ok.num_files >= 500)].iterrows():
        add_finding("Tables & layout", "Small files (table state)", r.table_name,
                    f"{int(r.num_files)} files, {r.size_gb} GB, avg {r.avg_file_mb} MB/file; partitioned by [{r.partition_cols}]",
                    "OPTIMIZE now, then enable predictive optimisation. If partitioned on high-cardinality columns, "
                    "replace with liquid clustering.", 4 if r.num_files >= 10000 else 3, 4, "S")
    no_layout = d_ok[(d_ok.size_gb >= 10) & (d_ok.partition_cols == "") & (d_ok.clustering_cols == "") & (d_ok.cluster_by_auto != True)]
    if len(no_layout):
        add_finding("Tables & layout", "Large tables with no clustering or partitioning", fmt_list(no_layout.table_name),
                    f"{len(no_layout)} tables of 10 GB or more with no layout ({round(no_layout.size_gb.sum())} GB total)",
                    "ALTER TABLE ... CLUSTER BY AUTO (or on the key filter columns) so queries can skip files.", 3, 4, "S")
    managed = d_ok[d_ok.table_type == "MANAGED"]
    po_off = managed[~managed.predictive_optimization.astype(str).str.upper().str.startswith("ENABLE")]
    if len(po_off):
        add_finding("Maintenance", "Predictive optimisation not enabled on hot managed tables", fmt_list(po_off.table_name),
                    f"{len(po_off)} of {len(managed)} inspected managed tables: {po_off.predictive_optimization.unique().tolist()}",
                    "ALTER CATALOG/SCHEMA ... ENABLE PREDICTIVE OPTIMIZATION so OPTIMIZE, VACUUM and ANALYZE run automatically.", 4, 5, "S")
    unmaint = d_ok[(d_ok.optimize_runs == 0) & (d_ok.commits_in_window >= 10) & ~d_ok.predictive_optimization.astype(str).str.upper().str.startswith("ENABLE")]
    if len(unmaint):
        add_finding("Maintenance", "Frequently written tables never optimised", fmt_list(unmaint.table_name),
                    f"{len(unmaint)} tables with 10+ commits and 0 OPTIMIZE in {D} days",
                    "Enable predictive optimisation (preferred) or schedule OPTIMIZE.", 3, 4, "S")
    novac = d_ok[(d_ok.vacuum_runs == 0) & (d_ok.commits_in_window >= 10)]
    if len(novac):
        add_finding("Maintenance", "No VACUUM on frequently written tables", fmt_list(novac.table_name),
                    f"{len(novac)} tables with 10+ commits and 0 VACUUM in {D} days",
                    "Enable predictive optimisation or schedule VACUUM. Unvacuumed tables grow storage cost, and deleted "
                    "data persists (a retention/compliance risk).", 2, 3, "S")
    rewrites = d_ok[d_ok.full_rewrites >= 20]
    if len(rewrites):
        add_finding("Tables & layout", "Tables rebuilt with CREATE OR REPLACE repeatedly", fmt_list(rewrites.table_name),
                    f"{len(rewrites)} tables with 20+ full rewrites in {D} days",
                    "Switch to incremental MERGE/APPEND, or a materialised view / streaming table for derived data.", 2, 3, "M")
    notdelta = d_ok[~d_ok.format.isin(["delta"])]
    if len(notdelta):
        add_finding("Tables & layout", "Hot tables not in Delta format", fmt_list(notdelta.table_name),
                    f"Formats: {notdelta.format.unique().tolist()}", "Convert to Delta managed tables.", 3, 4, "M")

# Hot-table read findings (section 3 metrics, cross-checked against table state)
if len(hot_read):
    # Statement-level metrics are shared by every table in a join. Drop small tables (for example, dimensions) whose own
    # file count shows the files were read from a different table.
    small_tbls = set(detail[(detail.status == "OK") & (detail.num_files < 200)].table_name) if len(detail) and "num_files" in detail else set()
    hr = hot_read[~hot_read.table_name.isin(small_tbls)]
    bad = hr[(hr.queries >= 100) & (hr.avg_files_per_query >= 1000) & (hr.pruning_pct.fillna(0) < 20)]
    for _, r in bad.iterrows():
        add_finding("Query performance", "Hot table with poor data skipping", r.table_name,
                    f"{r.queries} queries, {r.avg_files_per_query:.0f} files/query, {r.pruning_pct}% files pruned, "
                    f"avg {r.avg_mb_per_file} MB/file, p95 {r.p95_sec}s, clients: {r.top_clients}",
                    "Apply liquid clustering (CLUSTER BY AUTO or on the common filter columns), then OPTIMIZE. "
                    "Check the BI/semantic model pushes filters down.", 4, 5 if r.queries >= 1000 else 4, "S")
    small = hr[(hr.avg_mb_per_file < 16) & (hr.avg_files_per_query >= 500) & ~hr.table_name.isin(bad.table_name)]
    for _, r in small.iterrows():
        add_finding("Tables & layout", "Hot table read as many small files", r.table_name,
                    f"{r.queries} queries, {r.avg_files_per_query:.0f} files/query at ~{r.avg_mb_per_file} MB/file (target 128 MB-1 GB)",
                    "Compact with OPTIMIZE and enable predictive optimisation. Fix the upstream write pattern.", 3, 4, "S")
# Catalog-level predictive optimisation setting
if len(inv):
    po_rows = []
    for c in sorted(inv[inv.table_type == "MANAGED"].table_catalog.unique())[:100]:
        try:
            info = {r[0]: r[1] for r in spark.sql(f"DESCRIBE CATALOG EXTENDED `{c}`").collect()}
            po_rows.append({"catalog": c, "predictive_optimization": info.get("Predictive Optimization", "n/a")})
        except Exception as e:
            po_rows.append({"catalog": c, "predictive_optimization": "SKIPPED: " + str(e).split("\n")[0][:80]})
    po_cat = pd.DataFrame(po_rows)
    if len(po_cat):
        display(po_cat)
        off = po_cat[po_cat.predictive_optimization.str.upper().str.startswith("DISABLE")]
        if len(off):
            add_finding("Maintenance", "Predictive optimisation disabled at catalog level", fmt_list(off.catalog),
                        f"{len(off)} of {len(po_cat)} catalogs with managed tables have PO disabled",
                        "ALTER CATALOG <c> ENABLE PREDICTIVE OPTIMIZATION (or inherit from the metastore).", 4, 5, "S")

# COMMAND ----------

# MAGIC %md ## 5. Maintenance: predictive optimisation history and manual OPTIMIZE/VACUUM/ANALYZE

# COMMAND ----------

po = maint = pd.DataFrame()
if ok("system.storage.predictive_optimization_operations_history"):
    po_f = uc("catalog_name", "schema_name")
    po = q("5a Predictive optimisation operations", f"""
        SELECT workspace_id, catalog_name, operation_type, operation_status,
               COUNT(*) AS operations, COUNT(DISTINCT table_id) AS tables, MAX(start_time) AS last_operation,
               ROUND(SUM(usage_quantity), 1) AS dbus
        FROM system.storage.predictive_optimization_operations_history
        WHERE start_time >= current_date() - INTERVAL {D} DAYS {ws()} {po_f}
        GROUP BY ALL ORDER BY operations DESC""")
    if len(po):
        tot = po.operations.sum()
        failed = po[~po.operation_status.str.upper().str.startswith("SUCCESS")]
        if len(failed):
            share = failed.operations.sum() / tot
            reasons = failed.groupby("operation_status").operations.sum().sort_values(ascending=False).to_dict()
            add_finding("Maintenance", "Predictive optimisation operations failing", fmt_list(sorted(failed.catalog_name.dropna().unique())),
                        f"{int(failed.operations.sum())} of {int(tot)} PO operations failed ({share:.0%}) on "
                        f"{int(failed.tables.sum())} tables: {reasons}",
                        "PRIVATE_LINK_SETUP_ERROR or STORAGE errors mean serverless can't reach storage. Configure a Network "
                        "Connectivity Config (NCC) with private endpoints to the storage accounts, and allow it on the storage "
                        "firewall. Re-check this table after the fix.",
                        5 if share >= 0.5 else 3, 5 if share >= 0.5 else 3, "S")
    else:
        add_finding("Maintenance", "Predictive optimisation has not run", "all catalogs" if not UC_FILTERED else UC_SCOPE,
                    f"No PO operations recorded in {D} days",
                    "Enable predictive optimisation at the account/metastore level (it only acts on UC managed tables). "
                    "If private networking is used, configure NCC for serverless first.", 4, 5, "S")

redacted = False
if ok("system.query.history"):
    try:
        redacted = spark.sql(f"""SELECT count_if(statement_text = '<REDACTED>') / count(*) >= 0.99 FROM system.query.history
                                 WHERE start_time >= current_date() - 1 {ws()}""").first()[0] or False
    except Exception:
        pass
if redacted:
    CHECK_LOG.append(dict(check="5b Maintenance and ingestion SQL commands", status="SKIPPED", rows=0, seconds=0,
                          error="statement_text is <REDACTED> for this user; re-run as an account/workspace admin"))
    print("5b skipped: statement_text is redacted for this user.")
elif ok("system.query.history"):
    maint = q("5b Maintenance and ingestion SQL commands", f"""
        WITH s AS (
          SELECT workspace_id, start_time, upper(ltrim(substr(statement_text, 1, 4000))) AS t
          FROM system.query.history
          WHERE start_time >= current_date() - INTERVAL {D} DAYS {ws()} AND execution_status = 'FINISHED')
        SELECT workspace_id,
               CASE WHEN t LIKE 'VACUUM%' THEN 'VACUUM'
                    WHEN t LIKE 'OPTIMIZE%' THEN 'OPTIMIZE'
                    WHEN t LIKE 'ANALYZE%' THEN 'ANALYZE'
                    WHEN t LIKE 'COPY INTO%' THEN 'COPY INTO'
                    WHEN t LIKE '%READ_FILES(%' OR t LIKE '%CLOUD_FILES(%' THEN 'READ_FILES / AUTO LOADER'
                    WHEN t LIKE '%STREAMING TABLE%' THEN 'STREAMING TABLE'
                    WHEN t LIKE '%MATERIALIZED VIEW%' THEN 'MATERIALIZED VIEW'
                    WHEN t LIKE 'CREATE OR REPLACE TABLE%' THEN 'CREATE OR REPLACE TABLE' END AS command,
               COUNT(*) AS runs, MAX(start_time) AS last_run
        FROM s GROUP BY ALL HAVING command IS NOT NULL ORDER BY runs DESC""")
    have = set(maint.command) if len(maint) else set()
    if "VACUUM" not in have and not len(po):
        add_finding("Maintenance", "No VACUUM activity visible", "all tables",
                    f"No VACUUM statements in SQL history and no PO in {D} days (classic-cluster jobs are not visible here)",
                    "Enable predictive optimisation. Confirm whether job code runs VACUUM.", 2, 3, "S")

# COMMAND ----------

# MAGIC %md ## 6. Ingestion patterns: pipelines, triggers and incremental loading

# COMMAND ----------

pipes = trig = pd.DataFrame()
if ok("system.lakeflow.pipelines"):
    pipes = q("6a Lakeflow pipelines (current)", f"""
        SELECT workspace_id, pipeline_type, COUNT(*) AS pipelines, concat_ws(', ', slice(collect_list(name), 1, 5)) AS examples
        FROM (SELECT * FROM system.lakeflow.pipelines WHERE 1=1 {ws()}
              QUALIFY row_number() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) = 1)
        WHERE delete_time IS NULL GROUP BY ALL ORDER BY pipelines DESC""")
if ok("system.lakeflow.jobs"):
    trig = q("6b Job trigger types (current jobs)", f"""
        SELECT workspace_id, coalesce(trigger_type, 'MANUAL / NONE') AS trigger_type, paused, COUNT(*) AS jobs
        FROM (SELECT * FROM system.lakeflow.jobs WHERE 1=1 {ws()}
              QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1)
        WHERE delete_time IS NULL GROUP BY ALL ORDER BY jobs DESC""")
event_driven = len(trig) and trig.trigger_type.isin(["FILE_ARRIVAL", "CONTINUOUS", "TABLE"]).any()
incremental_sql = len(maint) and maint.command.isin(["READ_FILES / AUTO LOADER", "COPY INTO", "STREAMING TABLE"]).any()
if len(trig) and not event_driven and not len(pipes) and not incremental_sql:
    add_finding("Ingestion", "Ingestion is schedule-polling only", "all jobs",
                f"No file-arrival, continuous or table triggers, no Lakeflow pipelines, no read_files/COPY INTO in {D} days",
                "Use Auto Loader (cloudFiles) or Lakeflow Declarative Pipelines for incremental ingestion, with "
                "file-arrival or table triggers instead of fixed polling schedules. Consider Lakeflow Connect for "
                "database/SaaS sources.", 2, 3, "M")

# COMMAND ----------

# MAGIC %md ## 7. Compute: runtime currency, access modes, policies and utilisation

# COMMAND ----------

# Approximate LTS end-of-support dates. Verify against the Databricks runtime support lifecycle page.
LTS_EOS = {"9.1": "2024-09-23", "10.4": "2025-03-18", "11.3": "2025-10-19", "12.2": "2026-03-01", "13.3": "2026-08-22",
           "14.3": "2027-02-01", "15.4": "2027-08-19", "16.4": "2028-05-09", "17.3": "2028-10-22"}


def dbr_status(v):
    m = re.match(r"^(\d+)\.(\d+)", str(v or ""))
    if not m:
        return "unknown"
    mm = f"{m.group(1)}.{m.group(2)}"
    if mm in LTS_EOS:
        eos = datetime.date.fromisoformat(LTS_EOS[mm])
        return "END OF SUPPORT" if eos < TODAY else ("EOS < 6 MONTHS" if (eos - TODAY).days < 183 else "supported LTS")
    return "END OF SUPPORT" if int(m.group(1)) < 16 else "non-LTS"


clus = pd.DataFrame()
if ok("system.compute.clusters"):
    clus = q("7a Clusters active in window (by source, runtime, access mode, policy)", f"""
        WITH c AS (
          SELECT * FROM system.compute.clusters WHERE 1=1 {ws()}
          QUALIFY row_number() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1)
        SELECT workspace_id, cluster_source, regexp_extract(dbr_version, '^([0-9]+\\\\.[0-9]+)', 1) AS dbr,
               coalesce(data_security_mode, 'NOT RECORDED') AS access_mode, policy_id IS NOT NULL AS has_policy,
               COUNT(*) AS clusters,
               SUM(CASE WHEN cluster_source IN ('UI', 'API') AND (auto_termination_minutes IS NULL OR auto_termination_minutes = 0
                        OR auto_termination_minutes > 120) THEN 1 ELSE 0 END) AS long_or_no_autoterm,
               concat_ws(', ', slice(collect_set(cluster_name), 1, 5)) AS examples
        FROM c
        WHERE (delete_time IS NULL OR delete_time >= current_date() - INTERVAL {D} DAYS)
          AND cluster_source NOT IN ('PIPELINE', 'PIPELINE_MAINTENANCE')
        GROUP BY ALL ORDER BY clusters DESC""")

if len(clus):
    clus["dbr_status"] = clus.dbr.map(dbr_status)
    eos = clus[clus.dbr_status == "END OF SUPPORT"]
    if len(eos):
        g = eos.groupby(["cluster_source", "dbr"]).clusters.sum().to_dict()
        jobs_eos = int(eos[eos.cluster_source == "JOB"].clusters.sum())
        add_finding("Compute", "Clusters on end-of-support Databricks Runtime", fmt_list(eos.examples.unique(), 5),
                    f"{int(eos.clusters.sum())} clusters in {D} days on unsupported DBR ({jobs_eos} job clusters): {g}",
                    "Upgrade to a current LTS (16.4 or 17.3 LTS) in Standard or Dedicated access mode. Test with a "
                    "cloned job first.", 4, 5 if jobs_eos else 3, "M")
    soon = clus[clus.dbr_status == "EOS < 6 MONTHS"]
    if len(soon):
        add_finding("Compute", "Runtime reaching end of support within 6 months", fmt_list(soon.dbr.unique()),
                    f"{int(soon.clusters.sum())} clusters", "Plan the upgrade to 16.4/17.3 LTS now.", 3, 3, "M")
    legacy = clus[clus.access_mode.isin(["NONE", "NOT RECORDED", "LEGACY_TABLE_ACL", "LEGACY_PASSTHROUGH", "LEGACY_SINGLE_USER",
                                         "LEGACY_SINGLE_USER_STANDARD"])]
    if len(legacy):
        add_finding("Governance", "Clusters without a Unity Catalog access mode", fmt_list(legacy.examples.unique(), 5),
                    f"{int(legacy.clusters.sum())} clusters: {legacy.groupby('access_mode').clusters.sum().to_dict()}",
                    "Set Standard (USER_ISOLATION) or Dedicated (SINGLE_USER) access mode through cluster policies. "
                    "No-isolation clusters can't use UC.", 4, 3, "S")
    nopol = clus[(~clus.has_policy) & clus.cluster_source.isin(["UI", "API", "JOB"])]
    if len(nopol) and nopol.clusters.sum() >= 0.2 * clus.clusters.sum():
        add_finding("Compute", "Clusters created without a cluster policy", fmt_list(nopol.examples.unique(), 5),
                    f"{int(nopol.clusters.sum())} of {int(clus.clusters.sum())} clusters had no policy: "
                    f"{nopol.groupby('cluster_source').clusters.sum().to_dict()}",
                    "Enforce cluster policies (runtime, access mode, node types, auto-termination, tags) for all "
                    "interactive and job compute.", 3, 3, "S")
    at = clus[clus.long_or_no_autoterm > 0]
    if len(at):
        add_finding("Cost", "All-purpose clusters with no or long auto-termination", fmt_list(at.examples.unique(), 5),
                    f"{int(at.long_or_no_autoterm.sum())} clusters with auto-termination off or over 120 min",
                    "Set auto-termination to 30-60 min via policy, or move interactive work to serverless notebooks.", 2, 3, "S")

if ok("system.compute.node_timeline") and ok("system.compute.clusters"):
    util = q(f"7b Worker utilisation, last {NODE_D} days (top clusters by node-hours)", f"""
        WITH n AS (
          SELECT workspace_id, cluster_id, COUNT(*) / 60 AS node_hours,
                 AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu, percentile(cpu_user_percent + cpu_system_percent, 0.9) AS p90_cpu,
                 AVG(mem_used_percent) AS avg_mem
          FROM system.compute.node_timeline
          WHERE start_time >= current_date() - INTERVAL {NODE_D} DAYS {ws()} AND driver = false
          GROUP BY ALL),
        c AS (SELECT workspace_id, cluster_id, cluster_name, cluster_source, worker_node_type FROM system.compute.clusters WHERE 1=1 {ws()}
              QUALIFY row_number() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1)
        SELECT c.cluster_source, regexp_replace(c.cluster_name, '-run-[0-9]+$|-[0-9]{{6,}}$', '') AS cluster_family, c.worker_node_type,
               COUNT(*) AS clusters, ROUND(SUM(node_hours), 1) AS worker_node_hours,
               ROUND(AVG(avg_cpu), 1) AS avg_cpu_pct, ROUND(AVG(p90_cpu), 1) AS p90_cpu_pct, ROUND(AVG(avg_mem), 1) AS avg_mem_pct
        FROM n JOIN c USING (workspace_id, cluster_id)
        GROUP BY ALL ORDER BY worker_node_hours DESC LIMIT 25""")
    if len(util):
        low = util[(util.avg_cpu_pct < 30) & (util.avg_mem_pct < 50) & (util.worker_node_hours >= 50)]
        if len(low):
            add_finding("Cost", "Over-provisioned clusters (low CPU and memory)", fmt_list(low.cluster_family, 5),
                        f"{len(low)} cluster groups, {low.worker_node_hours.sum():.0f} worker node-hours in {NODE_D} days at "
                        f"~{low.avg_cpu_pct.mean():.0f}% CPU / {low.avg_mem_pct.mean():.0f}% memory",
                        "Right-size node types and max workers, or move jobs to serverless compute.", 2, 3, "S")

# COMMAND ----------

# MAGIC %md ## 8. Jobs: reliability, schedule overrun, cost, CI/CD and alerting

# COMMAND ----------

jobs = pd.DataFrame()
if ok("system.lakeflow.jobs") and ok("system.lakeflow.job_run_timeline"):
    cost_cte = ""
    cost_sel, cost_join = "CAST(NULL AS DOUBLE) AS list_usd", ""
    if ok("system.billing.usage") and ok("system.billing.list_prices"):
        cost_cte = f""",
        cost AS (
          SELECT u.workspace_id, u.usage_metadata.job_id AS job_id, SUM(u.usage_quantity * p.pricing.default) AS list_usd
          FROM system.billing.usage u
          JOIN system.billing.list_prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
               AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
          WHERE u.usage_date >= current_date() - {D} {ws('u.workspace_id')} AND u.usage_metadata.job_id IS NOT NULL
          GROUP BY ALL)"""
        cost_sel, cost_join = "ROUND(cost.list_usd, 0) AS list_usd", "LEFT JOIN cost USING (workspace_id, job_id)"
    jobs = q("8a Job health (runs, failures, skips, duration, cost)", f"""
        WITH j AS (
          SELECT * FROM system.lakeflow.jobs WHERE 1=1 {ws()}
          QUALIFY row_number() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1),
        runs AS (
          SELECT workspace_id, job_id, run_id,
                 max_by(result_state, period_end_time) AS result_state,
                 max_by(termination_code, period_end_time) AS termination_code,
                 SUM(coalesce(run_duration_seconds, 0)) AS run_sec,
                 MIN(period_start_time) AS started
          FROM system.lakeflow.job_run_timeline
          WHERE period_start_time >= current_date() - INTERVAL {D} DAYS {ws()}
          GROUP BY ALL),
        r AS (
          SELECT workspace_id, job_id, COUNT(*) AS runs,
                 SUM(CASE WHEN result_state IN ('FAILED', 'ERROR', 'TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
                 SUM(CASE WHEN result_state = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped,
                 ROUND(AVG(CASE WHEN result_state = 'SUCCEEDED' THEN run_sec END) / 60, 1) AS avg_min,
                 ROUND(percentile(CASE WHEN result_state = 'SUCCEEDED' THEN run_sec END, 0.95) / 60, 1) AS p95_min,
                 concat_ws(', ', slice(collect_set(CASE WHEN result_state IN ('FAILED', 'ERROR', 'SKIPPED') THEN termination_code END), 1, 3)) AS top_errors,
                 MAX(started) AS last_run
          FROM runs GROUP BY ALL){cost_cte}
        SELECT j.workspace_id, j.job_id, j.name, j.trigger_type, j.paused,
               j.deployment.kind AS deployment, size(coalesce(j.health_rules, array())) AS health_rules,
               coalesce(r.runs, 0) AS runs, r.failed, r.skipped,
               ROUND(100 * r.failed / nullif(r.runs, 0), 1) AS fail_pct, ROUND(100 * r.skipped / nullif(r.runs, 0), 1) AS skip_pct,
               r.avg_min, r.p95_min, r.top_errors, r.last_run, j.create_time, {cost_sel}
        FROM j LEFT JOIN r USING (workspace_id, job_id) {cost_join}
        WHERE j.delete_time IS NULL
        ORDER BY list_usd DESC NULLS LAST, runs DESC""", show=False)
    if len(jobs):
        display(jobs[jobs.runs > 0].head(50))

if len(jobs):
    active = jobs[jobs.runs > 0].copy()
    spend = active.list_usd.fillna(0)
    top_cost = set(active.assign(s=spend).sort_values("s", ascending=False).head(10).job_id)
    for _, r in active[(active.runs >= 10) & (active.fail_pct >= 5)].iterrows():
        add_finding("Jobs", "Job with high failure rate", f"{r['name']} ({r.job_id})",
                    f"{int(r.failed)} of {int(r.runs)} runs failed ({r.fail_pct}%), errors: {r.top_errors}, list {usd(r.list_usd)}",
                    "Fix the root cause. Add retries only for transient errors, plus job health rules and failure notifications.",
                    4 if r.fail_pct >= 20 else 3, 4 if r.job_id in top_cost else 2, "M")
    for _, r in active[(active.runs >= 10) & (active.skip_pct >= 5)].iterrows():
        add_finding("Jobs", "Job skipping runs (overrunning its schedule)", f"{r['name']} ({r.job_id})",
                    f"{int(r.skipped)} of {int(r.runs)} runs skipped ({r.skip_pct}%), avg {r.avg_min} / p95 {r.p95_min} min, "
                    f"list {usd(r.list_usd)}",
                    "The job takes longer than its trigger interval. Make it incremental (Auto Loader / Lakeflow pipelines), "
                    "move it to serverless to cut start-up time, or fix the layout of the tables it reads.",
                    4, 5 if r.job_id in top_cost else 3, "M")
    nodab = active[active.deployment.isna()]
    if len(nodab) and len(nodab) >= 0.5 * len(active):
        add_finding("DevOps", "Jobs not deployed with Declarative Automation Bundles (DABs)", fmt_list(nodab["name"], 8),
                    f"{len(nodab)} of {len(active)} active jobs are not bundle-deployed",
                    "Put production jobs under Databricks Asset Bundles with CI/CD (dev, test, prod targets).", 2, 3, "M")
    nohealth = active[active.health_rules == 0]
    if len(nohealth) and len(nohealth) >= 0.5 * len(active):
        add_finding("DevOps", "Jobs without health rules", fmt_list(nohealth["name"], 8),
                    f"{len(nohealth)} of {len(active)} active jobs have no duration/backlog health rules",
                    "Add health rules (RUN_DURATION_SECONDS, streaming backlog) and notifications on failure.", 2, 3, "S")
    cutoff = pd.Timestamp(TODAY - datetime.timedelta(days=D), tz="UTC")
    stale = jobs[(jobs.runs == 0) & (pd.to_datetime(jobs.create_time, utc=True) < cutoff)]
    if len(stale) >= 5:
        add_finding("DevOps", "Stale jobs (no runs in window)", fmt_list(stale["name"], 8),
                    f"{len(stale)} jobs created before the window with no runs in {D} days",
                    "Delete or archive unused jobs to reduce clutter and credential exposure.", 1, 2, "S")

if ok("system.lakeflow.job_run_timeline") and ok("system.compute.clusters"):
    ap = q("8b Jobs running on all-purpose clusters", f"""
        WITH r AS (
          SELECT DISTINCT workspace_id, job_id, explode(compute_ids) AS cluster_id
          FROM system.lakeflow.job_run_timeline
          WHERE period_start_time >= current_date() - INTERVAL {D} DAYS {ws()}),
        c AS (SELECT workspace_id, cluster_id, cluster_name, cluster_source FROM system.compute.clusters WHERE 1=1 {ws()}
              QUALIFY row_number() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1)
        SELECT r.workspace_id, c.cluster_name, COUNT(DISTINCT r.job_id) AS jobs
        FROM r JOIN c USING (workspace_id, cluster_id)
        WHERE c.cluster_source IN ('UI', 'API')
        GROUP BY ALL ORDER BY jobs DESC""")
    if len(ap):
        add_finding("Cost", "Scheduled jobs running on all-purpose clusters", fmt_list(ap.cluster_name, 5),
                    f"{int(ap.jobs.sum())} jobs used {len(ap)} all-purpose clusters",
                    "Use job clusters or serverless jobs. All-purpose DBUs cost more, and shared clusters mix workloads.", 2, 3, "S")

# COMMAND ----------

# MAGIC %md ## 9. SQL warehouses and query performance by client

# COMMAND ----------

if ok("system.compute.warehouses"):
    wh = q("9a SQL warehouses (current config)", f"""
        SELECT workspace_id, warehouse_id, warehouse_name, warehouse_type, warehouse_channel, warehouse_size,
               min_clusters, max_clusters, auto_stop_minutes
        FROM (SELECT * FROM system.compute.warehouses WHERE 1=1 {ws()}
              QUALIFY row_number() OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time DESC) = 1)
        WHERE delete_time IS NULL ORDER BY workspace_id, warehouse_name""")
    if len(wh):
        nonsl = wh[wh.warehouse_type.isin(["CLASSIC", "PRO"])]
        if len(nonsl):
            add_finding("SQL serving", "Non-serverless SQL warehouses", fmt_list(nonsl.warehouse_name),
                        f"{len(nonsl)} Classic/Pro warehouses", "Move to serverless SQL for fast start, IWM autoscaling and "
                        "short auto-stop.", 2, 3, "S")
        longstop = wh[(wh.auto_stop_minutes == 0) | (wh.auto_stop_minutes > 30)]
        if len(longstop):
            add_finding("Cost", "SQL warehouses with long or no auto-stop", fmt_list(longstop.warehouse_name),
                        f"Auto-stop: {dict(zip(longstop.warehouse_name, longstop.auto_stop_minutes))}",
                        "Set auto-stop to 5-10 min for serverless. Find what keeps the warehouse busy overnight "
                        "(see 9b hourly usage).", 2, 3, "S")
        prev = wh[wh.warehouse_channel == "PREVIEW"]
        if len(prev):
            add_finding("SQL serving", "SQL warehouses on the PREVIEW channel", fmt_list(prev.warehouse_name),
                        f"{len(prev)} warehouses on PREVIEW", "Use CURRENT for anything production-facing.", 2, 2, "S")

if ok("system.query.history"):
    whu = q("9b Warehouse workload (queueing, cache, spill, overnight activity)", f"""
        SELECT workspace_id, compute.warehouse_id AS warehouse_id, COUNT(*) AS queries,
               COUNT(DISTINCT client_application) AS clients,
               ROUND(SUM(waiting_at_capacity_duration_ms) / 3.6e6, 1) AS queued_hours,
               ROUND(100 * AVG(CASE WHEN from_result_cache THEN 1 ELSE 0 END), 1) AS result_cache_pct,
               SUM(CASE WHEN spilled_local_bytes > 0 THEN 1 ELSE 0 END) AS spilling_queries,
               ROUND(percentile(total_duration_ms, 0.95) / 1000, 1) AS p95_sec,
               ROUND(100 * AVG(CASE WHEN hour(start_time) BETWEEN 0 AND 5 THEN 1 ELSE 0 END), 1) AS pct_queries_00_06_utc
        FROM system.query.history
        WHERE start_time >= current_date() - INTERVAL {D} DAYS {ws()} AND compute.type = 'WAREHOUSE'
        GROUP BY ALL ORDER BY queries DESC""")
    if len(whu):
        for _, r in whu[whu.queued_hours >= 5].iterrows():
            add_finding("SQL serving", "Queries queueing at warehouse capacity", r.warehouse_id,
                        f"{r.queued_hours} h queued across {r.queries} queries in {D} days",
                        "Raise max clusters, or split workloads (app, BI, ad hoc) onto separate warehouses.", 3, 3, "S")
        for _, r in whu[(whu.spilling_queries >= 100)].iterrows():
            add_finding("SQL serving", "Queries spilling to disk", r.warehouse_id,
                        f"{r.spilling_queries} spilling queries", "Increase warehouse size for these queries, or tune joins "
                        "and aggregations.", 2, 2, "S")
        for _, r in whu[(whu.clients >= 4) & (whu.queries >= 10000)].iterrows():
            add_finding("SQL serving", "One warehouse serving many client types", r.warehouse_id,
                        f"{r.clients} client applications, {r.queries} queries, {r.pct_queries_00_06_utc}% between 00:00 and 06:00 UTC",
                        "Split application, BI and ad hoc traffic for isolation, chargeback and right-sized auto-stop.", 2, 3, "S")

    cli = q("9c Query performance by client application", f"""
        SELECT client_application, statement_type, COUNT(*) AS queries,
               ROUND(AVG(read_files)) AS avg_files_per_query,
               ROUND(SUM(read_bytes) / nullif(SUM(read_files), 0) / 1048576, 2) AS avg_mb_per_file,
               ROUND(100 * SUM(pruned_files) / nullif(SUM(pruned_files) + SUM(read_files), 0), 1) AS pruning_pct,
               ROUND(SUM(read_bytes) / power(1024, 4), 2) AS read_tb,
               ROUND(percentile(total_duration_ms, 0.95) / 1000, 1) AS p95_sec,
               ROUND(100 * AVG(CASE WHEN from_result_cache THEN 1 ELSE 0 END), 1) AS result_cache_pct
        FROM system.query.history
        WHERE start_time >= current_date() - INTERVAL {D} DAYS {ws()} AND execution_status = 'FINISHED'
          AND statement_type IN ('SELECT', 'INSERT', 'MERGE', 'UPDATE', 'DELETE')
        GROUP BY ALL HAVING queries >= 50 ORDER BY queries DESC""", limit=40)
    if len(cli):
        sel = cli[(cli.statement_type == "SELECT") & (cli.queries >= 1000) & (cli.avg_files_per_query >= 1000) & (cli.pruning_pct.fillna(0) < 20)]
        for _, r in sel.iterrows():
            add_finding("Query performance", "Client workload scanning without data skipping", r.client_application,
                        f"{r.queries} queries, {r.avg_files_per_query:.0f} files/query at {r.avg_mb_per_file} MB, "
                        f"{r.pruning_pct}% pruned, p95 {r.p95_sec}s",
                        "Fix the layout of the tables behind this workload (see section 3). For BI, add metric views / "
                        "materialised views and check filter pushdown (DirectQuery).", 4, 4, "M")
        dml = cli[cli.statement_type.isin(["UPDATE", "MERGE", "DELETE"]) & (cli.queries >= 500)]
        for cname, g in dml.groupby("client_application"):
            add_finding("Architecture", "Application issuing high-volume row-level DML", cname,
                        f"{int(g.queries.sum())} {'/'.join(g.statement_type)} statements in {D} days",
                        "OLTP-style writes suit Lakebase (managed Postgres) with sync to Delta better than a SQL warehouse.", 3, 3, "M")

# COMMAND ----------

# MAGIC %md ## 10. Cost: spend by workspace and product at list price

# COMMAND ----------

if ok("system.billing.usage") and ok("system.billing.list_prices"):
    cost = q("10a Spend by workspace and product (list $)", f"""
        SELECT u.workspace_id, u.billing_origin_product AS product,
               ROUND(SUM(u.usage_quantity), 0) AS dbus, ROUND(SUM(u.usage_quantity * p.pricing.default), 0) AS list_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
             AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
        WHERE u.usage_date >= current_date() - {D} {ws('u.workspace_id')}
        GROUP BY ALL HAVING list_usd > 0 ORDER BY list_usd DESC""")
    if len(cost):
        tot = cost.list_usd.sum()
        ap_usd = cost[cost["product"] == "ALL_PURPOSE"].list_usd.sum()
        if tot and ap_usd / tot >= 0.2:
            add_finding("Cost", "High share of all-purpose compute spend", "ALL_PURPOSE",
                        f"${ap_usd:,.0f} of ${tot:,.0f} ({ap_usd / tot:.0%}) in {D} days",
                        "Move scheduled work to job/serverless compute and interactive work to serverless notebooks.", 2, 3, "S")
        q("10b Monthly spend trend (list $)", f"""
            SELECT date_trunc('month', u.usage_date) AS month, u.billing_origin_product AS product,
                   ROUND(SUM(u.usage_quantity * p.pricing.default), 0) AS list_usd
            FROM system.billing.usage u
            LEFT JOIN system.billing.list_prices p ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
                 AND u.usage_end_time >= p.price_start_time AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
            WHERE u.usage_date >= current_date() - {D} {ws('u.workspace_id')}
            GROUP BY ALL HAVING list_usd > 0 ORDER BY month, list_usd DESC""")

# COMMAND ----------

# MAGIC %md ## 11. Ranked findings: what to fix, in priority order

# COMMAND ----------

cols = ["priority", "score", "category", "check", "object", "evidence", "recommendation", "severity", "impact", "effort"]
findings = pd.DataFrame(FINDINGS, columns=cols).sort_values(["score", "severity"], ascending=False).reset_index(drop=True)
# Keep the 8 highest-scoring rows per check and collapse the rest into one summary row, so the list stays actionable
MAX_PER_CHECK = 8
keep, extra = [], []
for chk, g in findings.groupby("check", sort=False):
    keep.append(g.head(MAX_PER_CHECK))
    rest = g.iloc[MAX_PER_CHECK:]
    if len(rest):
        top = rest.iloc[0].to_dict()
        top.update(object=fmt_list(rest.object, 15), evidence=f"{len(rest)} further objects with the same issue",
                   score=int(rest.score.max()) - 1)
        extra.append(top)
findings = pd.concat(keep + [pd.DataFrame(extra, columns=cols)], ignore_index=True)
findings["priority"] = findings.score.map(lambda x: "P1" if x >= 16 else "P2" if x >= 9 else "P3")
findings = findings.sort_values(["score", "severity"], ascending=False).reset_index(drop=True)
findings.insert(0, "rank", range(1, len(findings) + 1))
print(f"{len(findings)} findings: " + ", ".join(f"{p}={n}" for p, n in findings.priority.value_counts().sort_index().items()))
display(findings)

# COMMAND ----------

# MAGIC %md ### Summary by category, and check log (what ran, what was skipped)

# COMMAND ----------

if len(findings):
    display(findings.groupby(["category", "priority"]).size().unstack(fill_value=0).reset_index())
display(pd.DataFrame(CHECK_LOG))

# COMMAND ----------

if OUTPUT_TABLE and len(findings):
    out = spark.createDataFrame(findings.astype(str)).selectExpr("*", "current_timestamp() AS run_ts",
                                                                 f"{D} AS lookback_days", f"'{','.join(WS_IDS) or 'all'}' AS workspace_scope",
                                                                 f"'{UC_SCOPE}' AS table_scope")
    out.write.mode("append").option("mergeSchema", "true").saveAsTable(OUTPUT_TABLE)
    print(f"Saved {len(findings)} findings to {OUTPUT_TABLE}")
else:
    print("output_table not set: findings not saved (display above only).")

# COMMAND ----------

# Return a compact summary when run as a job (for example, to schedule this health check)
dbutils.notebook.exit(json.dumps({
    "findings": len(findings), "table_scope": UC_SCOPE, "by_priority": findings.priority.value_counts().to_dict() if len(findings) else {},
    "checks": CHECK_LOG, "deep_dive_status": detail.status.str[:90].value_counts().to_dict() if len(detail) else {}, "top10": findings.head(10)[["rank", "priority", "score", "check", "object"]].astype(str).to_dict("records")}))
