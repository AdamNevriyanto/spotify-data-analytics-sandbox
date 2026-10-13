# ============================================================
# cfzemp_pipeline.py — REFACTORED pipeline for employee table
#
# DESIGN PRINCIPLE:
#   YAML tells us WHAT the table looks like (columns, keys, read format).
#   spark.conf tells us WHERE the data lives (S3 bucket, JDBC host, catalog).
#   This script wires the two together.
#
# CHANGES FROM ORIGINAL TEMPLATE:
#
#   1. sys.path.insert → REMOVED
#      DAB's --editable install handles imports. Every dev had to
#      change the hardcoded path before — now it just works.
#
#   2. source_type from YAML → spark.conf.get("pipeline.source_type")
#      Switching S3↔JDBC is now a deploy-time decision (DAB target),
#      not a file edit. Dev can use S3, prod can use JDBC.
#
#   3. S3 path from YAML → constructed from spark.conf + source_table
#      spark.conf.get("pipeline.s3_bucket") + "/" + source_table + "/"
#      No hardcoded S3 paths in YAML or Python.
#
#   4. read_options flattened — was bronze_cfg["s3"]["read_options"]
#      Now bronze_cfg["read_options"] (we removed the s3:/jdbc: blocks
#      from YAML since those were infrastructure, not table definition).
#
#   5. JDBC reads connection from spark.conf, not YAML — host, port,
#      database come from DAB variables. Passwords from Secret Scope.
#      No connection details in any committed config file.
#
#   6. Clean if/else for S3 vs JDBC — no more commenting/uncommenting
#      code blocks. Runtime source_type drives the branch.
#
#   7. Unique function names — bronze_employee(), staged_employee().
#      SDP silently overwrites duplicate function names across scripts.
# ============================================================

import dlt
from pyspark.sql import functions as F

from utils.config_load import load_table_config, get_jdbc_options_from_conf
from utils.sdp_silver_transform import transform

# ---- Infrastructure config from spark.conf (set by DAB variables) ----
source_type = spark.conf.get("pipeline.source_type", "s3")

# ---- Table definition from YAML (columns, keys, cleansing, read format) ----
TABLE_NAME = "employee"
config = load_table_config(TABLE_NAME)
table_cfg = config["tables"][TABLE_NAME]
bronze_cfg = table_cfg["bronze"]
silver_cfg = table_cfg["silver"]


#####################################
##### ---- BRONZE LAYER ----- #######
#####################################

@dlt.table(
    name=f"bronze_{source_type}_{bronze_cfg['source_table']}",
    cluster_by=[bronze_cfg["cluster_by_col"]] if bronze_cfg.get("cluster_by_col") else None,
    table_properties={
        "delta.dataSkippingStatsColumns": bronze_cfg.get("cluster_by_col")
    },
)
def bronze_employee():
    """Ingest raw employee data (cfzemp) from S3 or JDBC."""

    source_table = bronze_cfg["source_table"]  # "cfzemp" — from YAML

    if source_type == "s3":
        # S3 path = DAB variable (bucket) + YAML (source_table name)
        s3_bucket = spark.conf.get("pipeline.s3_bucket")
        s3_path = f"{s3_bucket}/{source_table}/"

        df_raw = (
            spark.readStream.format("cloudFiles")
            .options(**bronze_cfg["read_options"])   # format, delimiter, etc. from YAML
            .load(s3_path)                           # path from spark.conf
        )
        source_label = F.col("_metadata.file_path")

    elif source_type == "jdbc":
        # Connection details from spark.conf, password from Secret Scope
        # get_jdbc_options_from_conf() is defined in config_load.py
        jdbc_opts = get_jdbc_options_from_conf(source_table)

        df_raw = (
            spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .load()
        )
        source_label = F.lit(source_table)

    else:
        raise ValueError(
            f"Unknown source_type '{source_type}'. "
            f"Set pipeline.source_type to 's3' or 'jdbc' in databricks.yml"
        )

    return (
        df_raw
        .withColumn("ingested_timestamp", F.current_timestamp())
        .withColumn("ingested_date", F.current_date())
        .withColumn("source_file", source_label)
    )


#####################################
##### ---- SILVER LAYER ----- #######
#####################################
# Silver layer has no infrastructure dependencies —
# it reads from the bronze table above, not from external sources.
# No changes needed here from the original pattern.

@dlt.view(name=f"silver_{TABLE_NAME}_staging")
@dlt.expect_all_or_drop(
    {f"valid_{pk}": f"`{pk}` IS NOT NULL" for pk in silver_cfg["primary_keys"]}
)
def staged_employee():
    """Apply column transforms and filter null PKs before merge."""
    df_bronze = dlt.read_stream(f"bronze_{source_type}_{bronze_cfg['source_table']}")
    return transform(df_bronze, table_cfg)


dlt.create_streaming_table(
    name=f"silver_{source_type}_{TABLE_NAME}",
    cluster_by=[silver_cfg["partition_columns"]] if silver_cfg.get("partition_columns") else None,
    table_properties={
        "delta.dataSkippingStatsColumns": silver_cfg.get("partition_columns")
    },
)

# Row ordering for SCD Type 1 merge:
# If incremental_columns defined (e.g. CFEDR6), use it + ingested_timestamp.
# Otherwise fall back to ingested_timestamp alone.
if silver_cfg.get("incremental_columns"):
    sequence_by = F.struct(
        F.col(silver_cfg["incremental_columns"][0]),
        F.col("ingested_timestamp"),
    )
else:
    sequence_by = F.col("ingested_timestamp")

dlt.apply_changes(
    target=f"silver_{source_type}_{TABLE_NAME}",
    source=f"silver_{TABLE_NAME}_staging",
    keys=silver_cfg["primary_keys"],
    sequence_by=sequence_by,
    stored_as_scd_type=1,
)
