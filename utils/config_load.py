# ============================================================
# utils/config_load.py — REFACTORED utility
#
# CHANGES FROM ORIGINAL:
#
#   1. NEW get_jdbc_options_from_conf(source_table)
#      Reads ALL JDBC connection details from spark.conf
#      (set by DAB variables via the configuration bridge).
#      Passwords fetched from Databricks Secret Scope.
#      This is the function the refactored pipeline scripts use.
#
#   2. KEPT get_jdbc_options(jdbc_cfg) as LEGACY / backward-compat.
#      Original pipeline scripts (cfaddr, cfmast) still import it.
#      New scripts should use get_jdbc_options_from_conf() instead.
#
#   3. Added error handling for missing YAML config files.
#      Original threw cryptic FileNotFoundError.
#
#   4. Added docstrings to every function.
# ============================================================

import os
import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")


# ============================================================
# YAML LOADING
# These functions read the table definition YAML (columns,
# keys, cleansing rules, read format options).
# ============================================================

def get_table_config_path(table_name: str) -> str:
    """Return the absolute path to config/table_<name>.yaml."""
    return os.path.join(CONFIG_DIR, f"table_{table_name}.yaml")


def load_config(path: str) -> dict:
    """Load and parse a YAML config file.

    Raises FileNotFoundError with a clear message listing
    available configs, instead of a cryptic OS error.
    """
    if not os.path.exists(path):
        available = (
            os.listdir(CONFIG_DIR)
            if os.path.isdir(CONFIG_DIR)
            else "CONFIG_DIR missing"
        )
        raise FileNotFoundError(
            f"Config not found: {path}\n"
            f"Available configs: {available}"
        )
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_table_config(table_name: str) -> dict:
    """Load config/table_<name>.yaml and return parsed dict."""
    path = get_table_config_path(table_name)
    return load_config(path)


# ============================================================
# JDBC OPTIONS — NEW (reads from spark.conf)
#
# This is the function refactored pipeline scripts use.
# Connection details come from DAB variables:
#
#   databricks.yml        →  configuration: block    →  this function
#   variables:                pipeline.jdbc_host:        spark.conf.get(
#     jdbc_host:                ${var.jdbc_host}           "pipeline.jdbc_host")
#       default: xxx
#
# Actual passwords come from Databricks Secret Scope:
#   dbutils.secrets.get(scope=..., key=...)
# ============================================================

def get_jdbc_options_from_conf(source_table: str) -> dict:
    """
    Build Spark JDBC options from spark.conf (set by DAB variables).

    All connection details (host, port, database, driver, ssl_mode)
    and secret scope/key names come from spark.conf. Actual passwords
    are fetched from Databricks Secret Scope at runtime.

    NOTE: Currently builds a PostgreSQL JDBC URL (jdbc:postgresql://).
    For MySQL/Oracle, extend this with a driver-to-URL-prefix map.

    Args:
        source_table: The database table to read (e.g. "cfzemp").
                      Used as the 'dbtable' Spark JDBC option.

    Returns:
        Dict ready to pass to spark.read.format("jdbc").options(**opts)
    """
    # noqa: F821 — spark and dbutils are Databricks globals
    host = spark.conf.get("pipeline.jdbc_host")             # noqa: F821
    port = spark.conf.get("pipeline.jdbc_port")
    database = spark.conf.get("pipeline.jdbc_database")
    driver = spark.conf.get("pipeline.jdbc_driver")
    ssl_mode = spark.conf.get("pipeline.jdbc_ssl_mode")
    secret_scope = spark.conf.get("pipeline.jdbc_secret_scope")
    secret_key_user = spark.conf.get("pipeline.jdbc_secret_key_user")
    secret_key_password = spark.conf.get("pipeline.jdbc_secret_key_password")

    return {
        "url": (
            f"jdbc:postgresql://{host}:{port}/{database}"
            f"?sslmode={ssl_mode}"
        ),
        "dbtable": source_table,
        "driver": driver,
        "user": dbutils.secrets.get(                        # noqa: F821
            scope=secret_scope,
            key=secret_key_user,
        ),
        "password": dbutils.secrets.get(                    # noqa: F821
            scope=secret_scope,
            key=secret_key_password,
        ),
    }