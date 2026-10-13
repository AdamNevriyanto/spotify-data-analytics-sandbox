import re
from pyspark.sql import DataFrame, functions as F, types as T

_TYPE_MAP = {
    "string": T.StringType(),
    "integer": T.IntegerType(),
    "timestamp": T.TimestampType(),
    "long": T.LongType(),
    "boolean": T.BooleanType(),
    "double": T.DoubleType(),
    "date": T.DateType()
}


def transform(df: DataFrame, table_cfg: dict) -> DataFrame:
    columns_cfg = table_cfg["columns"]

    # RENAME KOLOM SOURCE ke KOLOM TARGET yang ada di YAML
    for col in columns_cfg:
        if col["source"] != col["name"]:
            df = df.withColumnRenamed(col["source"], col["name"])

    # APPLY NULL MARKERS, TRIM
    for col in columns_cfg:
        name = col["name"]
        cleansing = col.get("cleansing", {})
        if name not in df.columns:
            continue
        if cleansing.get("trim"):
            df = df.withColumn(name, F.trim(F.col(name)))
        null_markers = cleansing.get("null_markers")
        if null_markers:
            df = df.withColumn(
                name,
                F.when(F.col(name).isin(null_markers), None).otherwise(F.col(name)),
            )

    # CASTING TIPE DATA
    for col in columns_cfg:
        name = col["name"]
        col_type = col["type"]
        date_format = col.get("format")
        if name in df.columns and col["type"] in _TYPE_MAP:
            if col_type == "date" and date_format:
                df = df.withColumn(name, F.to_date(F.col(name), date_format))
            elif col_type == "timestamp" and date_format:
                df = df.withColumn(name, F.to_timestamp(F.col(name), date_format))
            else:
                df = df.withColumn(name, F.col(name).cast(_TYPE_MAP[col_type]))
            # df = df.withColumn(name, F.col(name).cast(_TYPE_MAP[col["type"]]))

    return df

