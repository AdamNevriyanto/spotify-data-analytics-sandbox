# Databricks notebook source
# DBTITLE 1,DAB Pipeline Integration - Tutorial Guide
# MAGIC %md
# MAGIC # DAB Pipeline Integration — Tutorial Guide
# MAGIC
# MAGIC **Project**: `dbx_pipeline_integration`  
# MAGIC **Pattern**: Config-driven Bronze → Silver ETL using Spark Declarative Pipelines (SDP)  
# MAGIC **Author**: Data Engineering Team  
# MAGIC **Last Updated**: 2026-09-13
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## What This Project Does
# MAGIC
# MAGIC This project implements a **parameterized ETL pipeline** using Declarative Automation Bundles (DABs). Each source table (employee, customer, address) gets its own independent pipeline that:
# MAGIC
# MAGIC 1. **Bronze**: Ingests raw data from S3 (Auto Loader) or JDBC (PostgreSQL)
# MAGIC 2. **Silver**: Applies column transforms, type casting, null filtering, and SCD Type 1 merge
# MAGIC
# MAGIC ### Key Innovation: Separation of Concerns
# MAGIC
# MAGIC | Layer | File | Answers | Example |
# MAGIC | --- | --- | --- | --- |
# MAGIC | **Table definition** | `config/table_xxx.yaml` | "What does my table look like?" | Columns, types, PKs, read options |
# MAGIC | **Infrastructure** | `databricks.yml` | "Where does the data live?" | S3 bucket, JDBC host, catalog, schema |
# MAGIC | **Pipeline logic** | `pipelines/.../xxx_pipeline.py` | "How do I wire them together?" | Bronze ingestion + Silver transform |
# MAGIC | **Resource config** | `resources/pipeline_xxx.pipeline.yml` | "How is this deployed?" | Pipeline name, libraries, permissions |
# MAGIC
# MAGIC ### Design Principles
# MAGIC
# MAGIC * **YAML = table definition only** — columns, types, cleansing, PKs, read\_options. NOT: catalog, S3 paths, JDBC connections
# MAGIC * **databricks.yml = infrastructure config** — S3 bucket, JDBC host/port/db, catalog, schema, source\_type
# MAGIC * **configuration: block = the bridge** — maps DAB `${var.xxx}` → `pipeline.xxx` Spark config keys
# MAGIC * **spark.conf.get() = runtime reads** — Python reads infrastructure from Spark config at runtime
# MAGIC * **Secrets stay in Secret Scope** — only scope/key NAMES are committed, never actual passwords

# COMMAND ----------

# DBTITLE 1,Section 2: Project Folder Structure
# MAGIC %md
# MAGIC ## Section 2: Project Folder Structure
# MAGIC
# MAGIC ```
# MAGIC feature-adam_dbx-pipeline-integration/
# MAGIC │
# MAGIC ├── databricks.yml                              ← Variables + targets ONLY (don't touch per-table)
# MAGIC ├── pyproject.toml                              ← Python packaging (exposes utils/ for import)
# MAGIC │
# MAGIC ├── config/                                     ← TABLE DEFINITIONS (one per table)
# MAGIC │   ├── table_employee.yaml                      ← Columns, types, PKs, read_options for employee
# MAGIC │   ├── table_customer.yaml                      ← Same for customer
# MAGIC │   └── table_address.yaml                       ← Same for address
# MAGIC │
# MAGIC ├── utils/                                      ← SHARED UTILITIES
# MAGIC │   ├── config_load.py                           ← YAML loader + JDBC helpers
# MAGIC │   └── sdp_silver_transform.py                  ← Column rename, trim, cast, null markers
# MAGIC │
# MAGIC ├── pipelines/                                  ← PIPELINE SCRIPTS (grouped by layer)
# MAGIC │   ├── bronze_silver/                           ← Per-table ingestion + cleansing (flat, one script per table)
# MAGIC │   │   ├── cfzemp_pipeline.py                   ← Bronze + Silver for employee
# MAGIC │   │   ├── cfmast_pipeline.py                   ← Bronze + Silver for customer (future)
# MAGIC │   │   └── cfaddr_pipeline.py                   ← Bronze + Silver for address (future)
# MAGIC │   └── gold/                                    ← Business layer (future)
# MAGIC │       └── customer_360_pipeline.py              ← Joins multiple silver tables
# MAGIC │
# MAGIC ├── resources/                                  ← PIPELINE RESOURCE DEFINITIONS (one per pipeline)
# MAGIC │   ├── pipeline_employee.pipeline.yml            ← Deploys bronze_silver/cfzemp_pipeline.py
# MAGIC │   ├── pipeline_customer.pipeline.yml            ← (future)
# MAGIC │   └── pipeline_customer_360.pipeline.yml        ← (future)
# MAGIC │
# MAGIC └── .github/workflows/
# MAGIC     └── prod_deploy.yml                          ← CI/CD: deploys on merge to main
# MAGIC ```
# MAGIC
# MAGIC ### Why This Structure?
# MAGIC
# MAGIC * **Adding a new table** = 3 new files (config YAML + pipeline script + resource YAML). No editing shared files.
# MAGIC * **`resources/*.yml`** is auto-included via `include: - resources/*.yml` in `databricks.yml`.
# MAGIC * **Each resource file = one independent pipeline** in Databricks (own ID, own DAG, own runs).
# MAGIC * **`pipelines/bronze_silver/` vs `pipelines/gold/`** separates per-table ingestion from cross-table business logic.

# COMMAND ----------

# DBTITLE 1,Section 3: The Variable Flow (How Parameterization Works)
# MAGIC %md
# MAGIC ## Section 3: The Variable Flow (How Parameterization Works)
# MAGIC
# MAGIC This is the core innovation. Variables flow through **3 layers** before reaching your Python code:
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────┐     ┌──────────────────────────────┐     ┌─────────────────────────────────┐
# MAGIC │   LAYER 1: DECLARE      │     │   LAYER 2: BRIDGE              │     │   LAYER 3: READ                  │
# MAGIC │   databricks.yml        │     │   pipeline_xxx.pipeline.yml    │     │   xxx_pipeline.py                │
# MAGIC │                         │     │                                │     │                                   │
# MAGIC │   variables:            │     │   configuration:                │     │   spark.conf.get(                 │
# MAGIC │     s3_bucket:           │ →→→ │     pipeline.s3_bucket:          │ →→→ │     "pipeline.s3_bucket")          │
# MAGIC │       default: s3://...  │     │       ${var.s3_bucket}          │     │   # returns "s3://..."            │
# MAGIC │                         │     │                                │     │                                   │
# MAGIC │   (deploy-time values)  │     │   (Spark config keys)          │     │   (runtime reads)                │
# MAGIC └───────────────────────┘     └──────────────────────────────┘     └─────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Why 3 Layers?
# MAGIC
# MAGIC * `${var.xxx}` is a **deploy-time** substitution — it only works in YAML files that DABs processes
# MAGIC * Python files **never see** `${var.xxx}` — they run at pipeline execution time, not deploy time
# MAGIC * The `configuration:` block is the **bridge** that converts deploy-time values into runtime Spark config
# MAGIC
# MAGIC ### Concrete Example: S3 Bucket
# MAGIC
# MAGIC **Layer 1** — `databricks.yml` declares the variable with a default:
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   s3_bucket:
# MAGIC     description: S3 bucket base path for raw data
# MAGIC     default: s3://sandbox-dbx/dbx-pipeline-integration/raw
# MAGIC ```
# MAGIC
# MAGIC **Layer 2** — `pipeline_employee.pipeline.yml` bridges it to Spark config:
# MAGIC ```yaml
# MAGIC configuration:
# MAGIC   pipeline.s3_bucket: ${var.s3_bucket}
# MAGIC ```
# MAGIC At deploy time, DABs replaces `${var.s3_bucket}` with the actual value and saves it as a pipeline Spark config key.
# MAGIC
# MAGIC **Layer 3** — `cfzemp_pipeline.py` reads it at runtime:
# MAGIC ```python
# MAGIC s3_bucket = spark.conf.get("pipeline.s3_bucket")
# MAGIC s3_path = f"{s3_bucket}/{source_table}/"
# MAGIC # Result: "s3://sandbox-dbx/dbx-pipeline-integration/raw/cfzemp/"
# MAGIC ```
# MAGIC
# MAGIC ### All 13 Variables
# MAGIC
# MAGIC | Variable | Default | Purpose | Used by |
# MAGIC | --- | --- | --- | --- |
# MAGIC | `catalog` | mii\_workspace | Unity Catalog catalog | Table creation |
# MAGIC | `schema_bronze` | default | Bronze layer schema | Bronze tables |
# MAGIC | `schema_silver` | default | Silver layer schema | Silver tables |
# MAGIC | `source_type` | s3 | S3 or JDBC | if/else branch |
# MAGIC | `s3_bucket` | s3://sandbox-dbx/.../raw | S3 base path | Auto Loader |
# MAGIC | `jdbc_host` | dbx-sandbox-db.xxx | DB hostname | JDBC URL |
# MAGIC | `jdbc_port` | 5432 | DB port | JDBC URL |
# MAGIC | `jdbc_database` | postgres | DB name | JDBC URL |
# MAGIC | `jdbc_driver` | org.postgresql.Driver | JDBC driver class | JDBC connection |
# MAGIC | `jdbc_ssl_mode` | require | SSL setting | JDBC URL |
# MAGIC | `jdbc_secret_scope` | jdbc-scope | Secret scope name | dbutils.secrets.get() |
# MAGIC | `jdbc_secret_key_user` | db-username | Secret key for username | dbutils.secrets.get() |
# MAGIC | `jdbc_secret_key_password` | db-password | Secret key for password | dbutils.secrets.get() |
# MAGIC
# MAGIC ### Important Rule
# MAGIC
# MAGIC If you add a new variable to `databricks.yml`, you **MUST** also add it to the `configuration:` block in the resource YAML. Otherwise Python can't read it. And vice versa — if you reference `${var.xxx}` in the resource YAML but don't declare it in `databricks.yml`, the deploy fails.

# COMMAND ----------

# DBTITLE 1,Section 4: How to Add a New Table (Step-by-Step)
# MAGIC %md
# MAGIC ## Section 4: How to Add a New Table (Step-by-Step)
# MAGIC
# MAGIC Adding a new table requires **exactly 3 files**. No changes to `databricks.yml`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: Create the Table Config YAML
# MAGIC
# MAGIC **File**: `config/table_<TABLE_NAME>.yaml`
# MAGIC
# MAGIC This defines WHAT your table looks like — columns, types, keys, cleansing rules, read options.
# MAGIC
# MAGIC ```yaml
# MAGIC # config/table_invoice.yaml
# MAGIC # ============================================================
# MAGIC # TABLE DEFINITION ONLY — no infrastructure (S3, JDBC, catalog)
# MAGIC # ============================================================
# MAGIC tables:
# MAGIC   invoice:
# MAGIC     bronze:
# MAGIC       source_table: "cfinvc"              # The actual source table/file name
# MAGIC       cluster_by_col: INVDATE             # Delta clustering column
# MAGIC
# MAGIC       # Auto Loader read options (for S3 source)
# MAGIC       read_options:
# MAGIC         "cloudFiles.format": csv
# MAGIC         header: "true"
# MAGIC         delimiter: ","
# MAGIC         inferColumnTypes: "false"
# MAGIC         rescuedDataColumn: _rescued_data
# MAGIC         schemaEvolutionMode: addNewColumns
# MAGIC         schemaHints: "INVID STRING"       # Force types for tricky columns
# MAGIC
# MAGIC     silver:
# MAGIC       primary_keys:
# MAGIC         - INVID                           # Must match a column NAME (not source)
# MAGIC       incremental_columns:
# MAGIC         - INVDATE                         # For SCD1 ordering
# MAGIC       partition_columns: "INVDATE"
# MAGIC
# MAGIC     # Column definitions: rename, clean, and cast
# MAGIC     columns:
# MAGIC       - name: INVID                       # Final column name in silver
# MAGIC         source: "INVID"                   # Source column name (if different)
# MAGIC         type: string
# MAGIC         trim: true
# MAGIC         null_markers:
# MAGIC           - ""
# MAGIC           - "NULL"
# MAGIC
# MAGIC       - name: INVDATE
# MAGIC         type: date
# MAGIC         date_format: "yyyyMMdd"           # For date casting
# MAGIC         trim: true
# MAGIC
# MAGIC       - name: AMOUNT
# MAGIC         type: double
# MAGIC         trim: true
# MAGIC
# MAGIC       - name: CUSTID
# MAGIC         source: "CUST#"                   # Renames CUST# to CUSTID
# MAGIC         type: string
# MAGIC         trim: true
# MAGIC         null_markers:
# MAGIC           - ""
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Create the Pipeline Script
# MAGIC
# MAGIC **File**: `pipelines/bronze_silver/<source>_pipeline.py`
# MAGIC
# MAGIC ```python
# MAGIC # pipelines/bronze_silver/cfinvc_pipeline.py
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC from utils.config_load import load_table_config, get_jdbc_options_from_conf
# MAGIC from utils.sdp_silver_transform import transform
# MAGIC
# MAGIC # ---- Infrastructure from spark.conf ----
# MAGIC source_type = spark.conf.get("pipeline.source_type", "s3")
# MAGIC
# MAGIC # ---- Table definition from YAML ----
# MAGIC TABLE_NAME = "invoice"                    # <-- CHANGE THIS
# MAGIC config = load_table_config(TABLE_NAME)
# MAGIC table_cfg = config["tables"][TABLE_NAME]
# MAGIC bronze_cfg = table_cfg["bronze"]
# MAGIC silver_cfg = table_cfg["silver"]
# MAGIC
# MAGIC
# MAGIC #####################################
# MAGIC ##### ---- BRONZE LAYER ----- #######
# MAGIC #####################################
# MAGIC
# MAGIC @dlt.table(
# MAGIC     name=f"bronze_{source_type}_{bronze_cfg['source_table']}",
# MAGIC     cluster_by=[bronze_cfg["cluster_by_col"]] if bronze_cfg.get("cluster_by_col") else None,
# MAGIC     table_properties={
# MAGIC         "delta.dataSkippingStatsColumns": bronze_cfg.get("cluster_by_col")
# MAGIC     },
# MAGIC )
# MAGIC def bronze_invoice():                     # <-- UNIQUE function name
# MAGIC     """Ingest raw invoice data from S3 or JDBC."""
# MAGIC     source_table = bronze_cfg["source_table"]
# MAGIC
# MAGIC     if source_type == "s3":
# MAGIC         s3_bucket = spark.conf.get("pipeline.s3_bucket")
# MAGIC         s3_path = f"{s3_bucket}/{source_table}/"
# MAGIC         df_raw = (
# MAGIC             spark.readStream.format("cloudFiles")
# MAGIC             .options(**bronze_cfg["read_options"])
# MAGIC             .load(s3_path)
# MAGIC         )
# MAGIC         source_label = F.col("_metadata.file_path")
# MAGIC
# MAGIC     elif source_type == "jdbc":
# MAGIC         jdbc_opts = get_jdbc_options_from_conf(source_table)
# MAGIC         df_raw = spark.read.format("jdbc").options(**jdbc_opts).load()
# MAGIC         source_label = F.lit(source_table)
# MAGIC
# MAGIC     else:
# MAGIC         raise ValueError(f"Unknown source_type '{source_type}'")
# MAGIC
# MAGIC     return (
# MAGIC         df_raw
# MAGIC         .withColumn("ingested_timestamp", F.current_timestamp())
# MAGIC         .withColumn("ingested_date", F.current_date())
# MAGIC         .withColumn("source_file", source_label)
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC #####################################
# MAGIC ##### ---- SILVER LAYER ----- #######
# MAGIC #####################################
# MAGIC
# MAGIC @dlt.view(name=f"silver_{TABLE_NAME}_staging")
# MAGIC @dlt.expect_all_or_drop(
# MAGIC     {f"valid_{pk}": f"`{pk}` IS NOT NULL" for pk in silver_cfg["primary_keys"]}
# MAGIC )
# MAGIC def staged_invoice():                     # <-- UNIQUE function name
# MAGIC     """Apply transforms and filter null PKs."""
# MAGIC     df_bronze = dlt.read_stream(f"bronze_{source_type}_{bronze_cfg['source_table']}")
# MAGIC     return transform(df_bronze, table_cfg)
# MAGIC
# MAGIC
# MAGIC dlt.create_streaming_table(
# MAGIC     name=f"silver_{source_type}_{TABLE_NAME}",
# MAGIC     cluster_by=[silver_cfg["partition_columns"]] if silver_cfg.get("partition_columns") else None,
# MAGIC     table_properties={
# MAGIC         "delta.dataSkippingStatsColumns": silver_cfg.get("partition_columns")
# MAGIC     },
# MAGIC )
# MAGIC
# MAGIC if silver_cfg.get("incremental_columns"):
# MAGIC     sequence_by = F.struct(
# MAGIC         F.col(silver_cfg["incremental_columns"][0]),
# MAGIC         F.col("ingested_timestamp"),
# MAGIC     )
# MAGIC else:
# MAGIC     sequence_by = F.col("ingested_timestamp")
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target=f"silver_{source_type}_{TABLE_NAME}",
# MAGIC     source=f"silver_{TABLE_NAME}_staging",
# MAGIC     keys=silver_cfg["primary_keys"],
# MAGIC     sequence_by=sequence_by,
# MAGIC     stored_as_scd_type=1,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### What You MUST Change per Table
# MAGIC
# MAGIC | Item | Example | Why |
# MAGIC | --- | --- | --- |
# MAGIC | `TABLE_NAME = "invoice"` | Must match YAML key | Loads the right config |
# MAGIC | `def bronze_invoice()` | Unique function name | SDP silently overwrites duplicates |
# MAGIC | `def staged_invoice()` | Unique function name | Same reason |
# MAGIC | Docstrings | Describe the table | For readability |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Create the Resource YAML
# MAGIC
# MAGIC **File**: `resources/pipeline_<TABLE_NAME>.pipeline.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC # resources/pipeline_invoice.pipeline.yml
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     pipeline_invoice:                       # <-- Resource key (used in CLI)
# MAGIC       name: pipeline_invoice                # <-- Display name in UI
# MAGIC       catalog: ${var.catalog}
# MAGIC       schema: ${var.schema_bronze}
# MAGIC       serverless: true
# MAGIC       photon: true
# MAGIC       channel: current
# MAGIC       continuous: false
# MAGIC       root_path: ../pipelines/bronze_silver
# MAGIC
# MAGIC       libraries:
# MAGIC         - glob:
# MAGIC             include: ../pipelines/bronze_silver/cfinvc_pipeline.py
# MAGIC
# MAGIC       environment:
# MAGIC         dependencies:
# MAGIC           - --editable ${workspace.file_path}
# MAGIC
# MAGIC       configuration:
# MAGIC         # Copy this entire block from pipeline_employee.pipeline.yml
# MAGIC         # All 13 variables must be listed here
# MAGIC         pipeline.catalog: ${var.catalog}
# MAGIC         pipeline.schema_bronze: ${var.schema_bronze}
# MAGIC         pipeline.schema_silver: ${var.schema_silver}
# MAGIC         pipeline.source_type: ${var.source_type}
# MAGIC         pipeline.s3_bucket: ${var.s3_bucket}
# MAGIC         pipeline.jdbc_host: ${var.jdbc_host}
# MAGIC         pipeline.jdbc_port: ${var.jdbc_port}
# MAGIC         pipeline.jdbc_database: ${var.jdbc_database}
# MAGIC         pipeline.jdbc_driver: ${var.jdbc_driver}
# MAGIC         pipeline.jdbc_ssl_mode: ${var.jdbc_ssl_mode}
# MAGIC         pipeline.jdbc_secret_scope: ${var.jdbc_secret_scope}
# MAGIC         pipeline.jdbc_secret_key_user: ${var.jdbc_secret_key_user}
# MAGIC         pipeline.jdbc_secret_key_password: ${var.jdbc_secret_key_password}
# MAGIC
# MAGIC       permissions:
# MAGIC         - level: CAN_VIEW
# MAGIC           group_name: users
# MAGIC ```
# MAGIC
# MAGIC ### Checklist: What to Change
# MAGIC
# MAGIC * [ ] `pipeline_invoice` → your table name (resource key + display name)
# MAGIC * [ ] `root_path` → `../pipelines/bronze_silver` (shared, same for all)
# MAGIC * [ ] `libraries glob` → `../pipelines/bronze_silver/<source>_pipeline.py` (specific script)
# MAGIC * [ ] `configuration:` block → copy as-is (all 13 variables)
# MAGIC
# MAGIC **Note**: Paths use `../` because resource files are in `resources/`, one level below the bundle root.

# COMMAND ----------

# DBTITLE 1,Section 5: Deploy and Test
# MAGIC %md
# MAGIC ## Section 5: Deploy and Test
# MAGIC
# MAGIC All CLI commands run from the **web terminal** or **local machine**. They must be executed from the folder containing `databricks.yml`.
# MAGIC
# MAGIC > **Note**: The Databricks CLI does NOT work in notebook cells on serverless compute. Use the web terminal (sidebar → `>_` icon).
# MAGIC
# MAGIC ```bash
# MAGIC # Navigate to bundle root
# MAGIC cd /Workspace/Users/<your-email>/feature-adam_dbx-pipeline-integration
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: Validate (dry run — checks config, no changes)
# MAGIC ```bash
# MAGIC databricks bundle validate --target personal
# MAGIC ```
# MAGIC This checks:
# MAGIC * All `${var.xxx}` can be resolved
# MAGIC * Library paths exist and are within the sync root
# MAGIC * YAML syntax is valid
# MAGIC * Resource definitions are well-formed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 2: Deploy (creates/updates pipelines in workspace)
# MAGIC ```bash
# MAGIC databricks bundle deploy --target personal
# MAGIC ```
# MAGIC **What happens:**
# MAGIC * Syncs files to workspace (source-linked in dev mode = no copies)
# MAGIC * Creates or updates pipeline definitions
# MAGIC * Sets permissions
# MAGIC * **Does NOT run any pipeline** — just sets up the infrastructure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 3: Run a specific pipeline
# MAGIC ```bash
# MAGIC databricks bundle run --target personal pipeline_employee
# MAGIC ```
# MAGIC `pipeline_employee` is the **resource key** from the resource YAML (not the filename, not the display name).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 4: Check summary
# MAGIC ```bash
# MAGIC databricks bundle summary --target personal
# MAGIC ```
# MAGIC Shows all deployed resources with their URLs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Targets Explained
# MAGIC
# MAGIC | Target | Mode | Who Uses It | Pipeline Prefix | S3 Bucket |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | `personal` | development | Each developer | `[dev your_name]` | sandbox |
# MAGIC | `dev` | production | CI on dev branch | (none) | sandbox |
# MAGIC | `prod` | production | CI on merge to main | (none) | production-datalake |
# MAGIC
# MAGIC * **Development mode**: auto-prefixes pipeline names with `[dev your_username]` — each developer gets isolated copies
# MAGIC * **Production mode**: no prefix, runs as service principal
# MAGIC
# MAGIC ### Deploy Lifecycle
# MAGIC
# MAGIC ```
# MAGIC databricks bundle validate   →  Check config (safe, no changes)
# MAGIC                 ↓
# MAGIC databricks bundle deploy     →  Create/update pipeline definitions (no data processed)
# MAGIC                 ↓
# MAGIC databricks bundle run        →  Actually execute the pipeline (processes data)
# MAGIC                 ↓
# MAGIC databricks bundle summary    →  Verify what's deployed
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 6: Switching Between S3 and JDBC
# MAGIC %md
# MAGIC ## Section 6: Switching Between S3 and JDBC
# MAGIC
# MAGIC The `source_type` variable controls which data source the pipeline reads from. **No code changes needed.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 1: Temporary Override (`--var`)
# MAGIC
# MAGIC ```bash
# MAGIC # Deploy with JDBC instead of S3 (this deploy only)
# MAGIC databricks bundle deploy --target personal --var="source_type=jdbc"
# MAGIC
# MAGIC # Next deploy without --var reverts back to S3
# MAGIC databricks bundle deploy --target personal
# MAGIC ```
# MAGIC
# MAGIC You can override multiple variables at once:
# MAGIC ```bash
# MAGIC databricks bundle deploy --target personal \
# MAGIC   --var="source_type=jdbc" \
# MAGIC   --var="jdbc_host=my-other-db.example.com"
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 2: Permanent per-target (edit `databricks.yml`)
# MAGIC
# MAGIC In the `targets:` section, add `source_type: jdbc` to a specific target:
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       source_type: jdbc               # prod always reads from database
# MAGIC       jdbc_host: prod-db.xxx.rds.amazonaws.com
# MAGIC       jdbc_secret_scope: jdbc-scope-prod
# MAGIC ```
# MAGIC
# MAGIC Now `--target prod` always uses JDBC, `--target personal` always uses S3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Method 3: Change the default
# MAGIC
# MAGIC In `databricks.yml`, change:
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   source_type:
# MAGIC     default: jdbc    # was "s3"
# MAGIC ```
# MAGIC
# MAGIC Now ALL targets use JDBC unless they override it back to `s3`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Happens in the Python Code
# MAGIC
# MAGIC ```python
# MAGIC source_type = spark.conf.get("pipeline.source_type", "s3")
# MAGIC
# MAGIC if source_type == "s3":
# MAGIC     # Auto Loader reads CSVs from S3
# MAGIC     s3_path = f"{spark.conf.get('pipeline.s3_bucket')}/{source_table}/"
# MAGIC     df_raw = spark.readStream.format("cloudFiles").options(**read_options).load(s3_path)
# MAGIC
# MAGIC elif source_type == "jdbc":
# MAGIC     # JDBC reads from PostgreSQL
# MAGIC     jdbc_opts = get_jdbc_options_from_conf(source_table)
# MAGIC     df_raw = spark.read.format("jdbc").options(**jdbc_opts).load()
# MAGIC ```
# MAGIC
# MAGIC The if/else is already in the pipeline script. Switching `source_type` just picks a different branch.
# MAGIC
# MAGIC ### Prerequisites for JDBC
# MAGIC
# MAGIC Before JDBC works, you need:
# MAGIC 1. Secret Scope created with DB credentials (see Section 7)
# MAGIC 2. Database reachable from Databricks workspace (firewall/security group)
# MAGIC 3. PostgreSQL driver (included by default on serverless)

# COMMAND ----------

# DBTITLE 1,Section 7: Secret Scope Setup
# MAGIC %md
# MAGIC ## Section 7: Secret Scope Setup
# MAGIC
# MAGIC Secret scopes store sensitive credentials (DB passwords, API keys) securely in Databricks. Your pipeline code reads them at runtime via `dbutils.secrets.get()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Step 1: Create the Secret Scope (one-time)
# MAGIC
# MAGIC From the **web terminal** or **local machine**:
# MAGIC ```bash
# MAGIC databricks secrets create-scope --scope jdbc-scope
# MAGIC ```
# MAGIC
# MAGIC ### Step 2: Add Credentials
# MAGIC ```bash
# MAGIC databricks secrets put-secret --scope jdbc-scope --key db-username --string-value "your_db_username"
# MAGIC databricks secrets put-secret --scope jdbc-scope --key db-password --string-value "your_db_password"
# MAGIC ```
# MAGIC
# MAGIC ### Step 3: Verify (lists keys, NOT values)
# MAGIC ```bash
# MAGIC databricks secrets list-secrets --scope jdbc-scope
# MAGIC ```
# MAGIC Output:
# MAGIC ```
# MAGIC Key name    Last updated
# MAGIC ----------  ----------------
# MAGIC db-username 2026-09-13T10:00
# MAGIC db-password 2026-09-13T10:00
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### For Production: Separate Scope
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope --scope jdbc-scope-prod
# MAGIC databricks secrets put-secret --scope jdbc-scope-prod --key db-username --string-value "prod_username"
# MAGIC databricks secrets put-secret --scope jdbc-scope-prod --key db-password --string-value "prod_password"
# MAGIC ```
# MAGIC
# MAGIC The `prod` target in `databricks.yml` already points to `jdbc-scope-prod`:
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       jdbc_secret_scope: jdbc-scope-prod
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How the Pipeline Reads Secrets
# MAGIC
# MAGIC The full chain:
# MAGIC ```
# MAGIC databricks.yml                  pipeline resource YAML              config_load.py
# MAGIC ──────────────────                ──────────────────────              ────────────────
# MAGIC jdbc_secret_scope:              pipeline.jdbc_secret_scope:         scope = spark.conf.get(
# MAGIC   default: jdbc-scope     →       ${var.jdbc_secret_scope}    →       "pipeline.jdbc_secret_scope")
# MAGIC                                                                     password = dbutils.secrets.get(
# MAGIC                                                                       scope=scope, key=key)
# MAGIC ```
# MAGIC
# MAGIC **Only the scope name and key name travel through the config chain.** The actual password is NEVER in any file — it goes directly from Secret Scope to the JDBC connection at runtime.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Important Notes
# MAGIC
# MAGIC * **Secret values are write-only** — you can never read them back after creation, only overwrite
# MAGIC * **Scope names and key names are just identifiers** — safe to commit to Git
# MAGIC * **Each workspace has its own scopes** — dev and prod workspaces need separate setup
# MAGIC * **To update a password**: re-run `put-secret` with the new value (same scope + key)

# COMMAND ----------

# DBTITLE 1,Section 8: Git Collaboration Workflow
# MAGIC %md
# MAGIC ## Section 8: Git Collaboration Workflow
# MAGIC
# MAGIC ### The Golden Rule
# MAGIC
# MAGIC **`databricks.yml` is a shared file — don't touch it per-table.** Only a tech lead or dedicated PR should change it.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What Each Developer Pushes (per table)
# MAGIC
# MAGIC | # | File | Example |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | `config/table_<name>.yaml` | `config/table_invoice.yaml` |
# MAGIC | 2 | `pipelines/bronze_silver/<source>_pipeline.py` | `pipelines/bronze_silver/cfinvc_pipeline.py` |
# MAGIC | 3 | `resources/pipeline_<name>.pipeline.yml` | `resources/pipeline_invoice.pipeline.yml` |
# MAGIC
# MAGIC Three files. No conflicts with other developers because each file has a unique name.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Developer Workflow
# MAGIC
# MAGIC ```
# MAGIC 1. Create feature branch
# MAGIC    git checkout -b feature/add-table-invoice
# MAGIC
# MAGIC 2. Create 3 files (config YAML + pipeline script + resource YAML)
# MAGIC
# MAGIC 3. Test locally
# MAGIC    databricks bundle deploy --target personal
# MAGIC    databricks bundle run --target personal pipeline_invoice
# MAGIC
# MAGIC 4. Push and open PR
# MAGIC    git add config/table_invoice.yaml
# MAGIC    git add pipelines/bronze_silver/cfinvc_pipeline.py
# MAGIC    git add resources/pipeline_invoice.pipeline.yml
# MAGIC    git commit -m "Add invoice table pipeline"
# MAGIC    git push origin feature/add-table-invoice
# MAGIC
# MAGIC 5. PR reviewed and merged to main
# MAGIC    → CI/CD deploys to prod automatically
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Why There Are No Conflicts
# MAGIC
# MAGIC * `databricks.yml` — nobody edits it, no conflict
# MAGIC * `config/table_invoice.yaml` — unique filename, no conflict
# MAGIC * `pipelines/.../cfinvc_pipeline.py` — unique filename, no conflict
# MAGIC * `resources/pipeline_invoice.pipeline.yml` — unique filename, no conflict
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Development Isolation via `--target personal`
# MAGIC
# MAGIC When two developers both deploy with `--target personal`, development mode auto-prefixes:
# MAGIC
# MAGIC ```
# MAGIC Developer A:  [dev adam_nevriyanto] pipeline_invoice
# MAGIC Developer B:  [dev jeremi_santoso] pipeline_invoice
# MAGIC ```
# MAGIC
# MAGIC Completely separate pipeline instances. Different IDs, different DAGs, different runs. They never collide.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### "But deploy creates ALL pipelines!"
# MAGIC
# MAGIC Yes, `--target personal` deploys every resource in `resources/*.yml`. But:
# MAGIC * **Deploying is fast** — it just creates pipeline definitions, doesn't process data
# MAGIC * **Running is selective** — `databricks bundle run --target personal pipeline_invoice` runs ONLY that one
# MAGIC * **Extra pipelines sit idle** — they don't consume compute until you explicitly run them

# COMMAND ----------

# DBTITLE 1,Section 9: CLI Quick Reference
# MAGIC %md
# MAGIC ## Section 9: CLI Quick Reference
# MAGIC
# MAGIC All commands run from the bundle root (folder containing `databricks.yml`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Bundle Commands
# MAGIC
# MAGIC | Command | What It Does |
# MAGIC | --- | --- |
# MAGIC | `databricks bundle validate --target personal` | Dry run — checks config, no changes |
# MAGIC | `databricks bundle deploy --target personal` | Creates/updates pipeline definitions |
# MAGIC | `databricks bundle deploy --target personal --auto-approve` | Deploy + auto-confirm destructive changes |
# MAGIC | `databricks bundle run --target personal pipeline_employee` | Run a specific pipeline |
# MAGIC | `databricks bundle summary --target personal` | Show deployed resources + URLs |
# MAGIC
# MAGIC ### Variable Overrides
# MAGIC
# MAGIC | Command | What It Does |
# MAGIC | --- | --- |
# MAGIC | `--var="s3_bucket=s3://other/raw"` | Override one variable |
# MAGIC | `--var="source_type=jdbc" --var="jdbc_host=other.com"` | Override multiple |
# MAGIC | `--target prod` | Use prod target variables (permanent overrides) |
# MAGIC
# MAGIC ### Secret Scope Commands
# MAGIC
# MAGIC | Command | What It Does |
# MAGIC | --- | --- |
# MAGIC | `databricks secrets create-scope --scope jdbc-scope` | Create a new scope |
# MAGIC | `databricks secrets put-secret --scope jdbc-scope --key db-password --string-value "xxx"` | Add/update a secret |
# MAGIC | `databricks secrets list-scopes` | List all scopes |
# MAGIC | `databricks secrets list-secrets --scope jdbc-scope` | List keys in a scope (NOT values) |
# MAGIC | `databricks secrets delete-secret --scope jdbc-scope --key db-password` | Delete a secret |
# MAGIC
# MAGIC ### Where to Run These
# MAGIC
# MAGIC | Location | Works? | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | Web terminal (Databricks sidebar → `>_`) | Yes | Recommended |
# MAGIC | Local machine | Yes | Need `pip install databricks-cli` + auth |
# MAGIC | Notebook cell (`%sh`) on serverless | No | CLI blocked on serverless compute |
# MAGIC | Chat assistant | Yes | Just ask: "validate the bundle" |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Quick Start Cheat Sheet
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Navigate to bundle root
# MAGIC cd /Workspace/Users/<your-email>/feature-adam_dbx-pipeline-integration
# MAGIC
# MAGIC # 2. Validate (always do this first)
# MAGIC databricks bundle validate --target personal
# MAGIC
# MAGIC # 3. Deploy
# MAGIC databricks bundle deploy --target personal
# MAGIC
# MAGIC # 4. Run your table
# MAGIC databricks bundle run --target personal pipeline_employee
# MAGIC
# MAGIC # 5. Check what's deployed
# MAGIC databricks bundle summary --target personal
# MAGIC
# MAGIC # 6. Test with different S3 bucket
# MAGIC databricks bundle deploy --target personal --var="s3_bucket=s3://test-bucket/raw"
# MAGIC
# MAGIC # 7. Switch to JDBC
# MAGIC databricks bundle deploy --target personal --var="source_type=jdbc"
# MAGIC
# MAGIC # 8. Deploy to production (CI/CD does this, but you can do it manually)
# MAGIC databricks bundle deploy --target prod
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Section 10: Troubleshooting & FAQ
# MAGIC %md
# MAGIC ## Section 10: Troubleshooting & FAQ
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: "path ... is not contained in sync root path"
# MAGIC
# MAGIC **Cause**: A library glob path resolves outside the bundle root (usually a `../` going too far up).  
# MAGIC **Fix**: Check `root_path` and `libraries.glob.include` in your resource YAML. Paths in `resources/` are relative to the `resources/` folder, so use `../pipelines/...` to go up one level.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: "the deployment requires destructive actions"
# MAGIC
# MAGIC **Cause**: You renamed or removed a pipeline resource key. DABs wants to delete the old one.  
# MAGIC **Fix**: Add `--auto-approve` if you're OK with the deletion. Otherwise, keep the old resource key.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: Pipeline runs but bronze table is empty
# MAGIC
# MAGIC **Cause**: No data files in the S3 path.  
# MAGIC **Check**: Verify files exist at `s3://<bucket>/<source_table>/` (e.g., `s3://sandbox-dbx/.../raw/cfzemp/`).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: "Py4JJavaError: ... Secret does not exist"
# MAGIC
# MAGIC **Cause**: The secret scope or key doesn't exist yet.  
# MAGIC **Fix**: Create the scope and add secrets (see Section 7).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: Duplicate function names — silver table has wrong data
# MAGIC
# MAGIC **Cause**: Two pipeline scripts define functions with the same name (e.g., both have `def staged()`). SDP silently overwrites the first with the second.  
# MAGIC **Fix**: Every function name must be unique across ALL scripts in the pipeline. Use `def staged_employee()`, `def staged_invoice()`, etc.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: Can I run the CLI from a notebook cell?
# MAGIC
# MAGIC **No** — the Databricks CLI is blocked on serverless compute. Use:
# MAGIC * Web terminal (sidebar → `>_` icon)
# MAGIC * Local machine
# MAGIC * Chat assistant ("validate the bundle")
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: How do target variable overrides work?
# MAGIC
# MAGIC Target variables **replace** the defaults, they don't duplicate them. Only list what's **different** from the defaults in each target. Everything else keeps its default value.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Q: I added a variable but Python can't read it
# MAGIC
# MAGIC You probably forgot to add it to the `configuration:` block in the resource YAML. The chain is:
# MAGIC 1. Declare in `databricks.yml` `variables:` section
# MAGIC 2. Bridge in resource YAML `configuration:` block
# MAGIC 3. Read in Python via `spark.conf.get()`
# MAGIC
# MAGIC All three must be connected. Missing any link breaks the chain.

# COMMAND ----------

# DBTITLE 1,Future: Standard Spark Job Support
# MAGIC %md
# MAGIC ## Future Enhancement: Standard Spark Job Support
# MAGIC
# MAGIC > **STATUS**: Not yet implemented. This is a reminder for when we need standard Spark jobs alongside SDP pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Problem
# MAGIC
# MAGIC The current `config_load.py` reads infrastructure config via `spark.conf.get("pipeline.xxx")`. This works for **SDP pipelines** because the `configuration:` block in the resource YAML sets those Spark config keys.
# MAGIC
# MAGIC But **standard Spark jobs** (notebook tasks in Lakeflow Jobs) don't have a `configuration:` block. They use `base_parameters:` instead, which Python reads via `dbutils.widgets.get()`.
# MAGIC
# MAGIC ### The Solution: Dual-Context Helper
# MAGIC
# MAGIC Add a `get_param()` function to `config_load.py` that tries both:
# MAGIC
# MAGIC ```python
# MAGIC def get_param(key: str, default: str = None) -> str:
# MAGIC     """Read a parameter from spark.conf (SDP pipeline) or widgets (standard job)."""
# MAGIC     try:
# MAGIC         return spark.conf.get(f"pipeline.{key}")
# MAGIC     except Exception:
# MAGIC         return dbutils.widgets.get(key) if default is None else dbutils.widgets.get(key, default)
# MAGIC ```
# MAGIC
# MAGIC Then replace all `spark.conf.get("pipeline.xxx")` calls with `get_param("xxx")`.
# MAGIC
# MAGIC ### Standard Job Resource YAML Would Look Like
# MAGIC
# MAGIC ```yaml
# MAGIC # resources/job_employee_report.job.yml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     job_employee_report:
# MAGIC       name: job_employee_report
# MAGIC       tasks:
# MAGIC         - task_key: run_report
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../notebooks/employee_report.py
# MAGIC             base_parameters:                    # ← replaces configuration: block
# MAGIC               s3_bucket: ${var.s3_bucket}
# MAGIC               source_type: ${var.source_type}
# MAGIC               catalog: ${var.catalog}
# MAGIC ```
# MAGIC
# MAGIC ### Comparison
# MAGIC
# MAGIC | Feature | SDP Pipeline (current) | Standard Job (future) |
# MAGIC | --- | --- | --- |
# MAGIC | Config mechanism | `configuration:` block | `base_parameters:` |
# MAGIC | Python reads via | `spark.conf.get()` | `dbutils.widgets.get()` |
# MAGIC | Serverless | Yes | Yes |
# MAGIC | Streaming (Auto Loader) | Yes | No (batch only) |
# MAGIC | SCD merge | Built-in (`apply_changes`) | Manual (`MERGE INTO`) |
# MAGIC | Best for | Bronze→Silver ETL | Reports, ML training, batch processing |
# MAGIC
# MAGIC ### When to Implement
# MAGIC
# MAGIC Implement this when you need a standard Spark job (e.g., gold-layer reports, ML training, one-off batch jobs) that reuses the same `config_load.py` and DAB variables.