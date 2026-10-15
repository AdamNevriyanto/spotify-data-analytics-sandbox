
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

_ITEM = "_item"
_POS = "_pos"


def _resolve_path(exploded: DataFrame, path: str, optional: bool, col_name: str):
    """Bangun ekspresi untuk path relatif terhadap elemen array."""
    expr = F.expr(f"`{_ITEM}`.{path}")
    try:
        # select() hanya menguji apakah path bisa di-resolve (tidak menjalankan job)
        exploded.select(expr)
    except Exception as e:
        if optional:
            return None
        raise ValueError(
            f"Path '{path}' untuk kolom '{col_name}' tidak ditemukan di elemen array. "
            f"Cek penulisan path di YAML, atau tandai `optional: true` kalau field ini "
            f"memang boleh hilang. Detail: {e}"
        ) from e
    return expr


def explode_json(df: DataFrame, explode_cfg: dict, columns: list) -> DataFrame:
    """
    Filter -> posexplode array -> ambil kolom sesuai `path` di YAML.
    Hasilnya DataFrame datar: carry_columns + position_column + satu kolom per entri `path`.
    """
    array_path = explode_cfg.get("array_path")
    if not array_path:
        raise ValueError("explode.array_path wajib diisi di YAML.")

    carry = explode_cfg.get("carry_columns", [])
    position_column = explode_cfg.get("position_column")

    # 1) Filter baris induk (misalnya hanya item_type = artists)
    for col_name, expected in (explode_cfg.get("filter") or {}).items():
        values = expected if isinstance(expected, list) else [expected]
        df = df.filter(F.col(col_name).isin(values))

    # 2) Bongkar array: satu baris per elemen
    exploded = df.select(
        *[F.col(c) for c in carry],
        F.posexplode(F.col(array_path)).alias(_POS, _ITEM),
    )

    # 3) Susun kolom hasil
    select_cols = [F.col(c) for c in carry]
    if position_column:
        select_cols.append((F.col(_POS) + 1).alias(position_column))

    for col in columns:
        path = col.get("path")
        if not path:
            continue

        expr = _resolve_path(exploded, path, bool(col.get("optional", False)), col["name"])
        if expr is None:
            expr = F.lit(None).cast("string")
        elif col.get("array_join") is not None:
            expr = F.array_join(expr.cast("array<string>"), col["array_join"])

        select_cols.append(expr.alias(col["source"]))

    return exploded.select(*select_cols)