import dlt
from pyspark.sql import functions as F

from utils.config_load import load_table_config, get_jdbc_options_from_conf
from utils.sdp_silver_transform import transform
from utils.transform_json import explode_json

# ---- Infrastructure config from spark.conf (set by DAB variables) ----
source_type = spark.conf.get("pipeline.source_type", "s3")

# ---- Table definition from YAML (columns, keys, cleansing, read format) ----
TABLE_NAME = "top_items"
config = load_table_config(TABLE_NAME)
table_cfg = config["tables"][TABLE_NAME]
bronze_cfg = table_cfg["bronze"]
silver_conf = table_cfg["silver"]
BRONZE_TABLE = f"bronze_{source_type}_{bronze_cfg['source_table']}"

#####################################
##### ---- BRONZE LAYER ----- #######
#####################################

@dlt.table(
    name=BRONZE_TABLE,
    cluster_by=[bronze_cfg["cluster_by_col"]] if bronze_cfg.get("cluster_by_col") else None,
    table_properties={
        "delta.dataSkippingStatsColumns": bronze_cfg.get("cluster_by_col")
    },
)
def _bronze():
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

## BUILD SILVER TABLES FROM BRONZE (DENGAN NAMING SESUAI DI YAML)
def build_silver(name: str, silver_cfg: dict) -> None:
    t_cfg = {"bronze": bronze_cfg, "silver": silver_cfg, "columns": silver_cfg["columns"]}

    staging_view = f"silver_{name}_staging"
    target_table = f"silver_{source_type}_{name}"

    # TEMP TABLE SILVER STAGING UNTUK HASIL EXPLODE DAN CLEANSING
    @dlt.view(name=staging_view)
    @dlt.expect_all_or_drop(
        {f"valid_{pk}": f"`{pk}` IS NOT NULL" for pk in silver_cfg["primary_keys"]}
    )
    def _staged():
        # EXPLODE KOLOM JSON (payload.items) dan CLEANSING KOLOM
        df_bronze = dlt.read_stream(BRONZE_TABLE)
        if silver_cfg.get("explode"):                                       
            df = explode_json(df, silver_cfg["explode"], table_cfg["columns"])
        return transform(df_bronze, t_cfg)


    dlt.create_streaming_table(
        name=target_table,
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
        target=target_table,
        source=staging_view,
        keys=silver_cfg["primary_keys"],
        sequence_by=sequence_by,
        stored_as_scd_type=1,
    )

for _name, _silver_cfg in silver_conf.items():
    build_silver(_name, _silver_cfg)