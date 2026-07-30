"""Doris 元数据发现：库/表/列（information_schema，MySQL 协议）。"""
from __future__ import annotations

_EXCLUDED = ("information_schema", "mysql", "__internal_schema")


def discover(conn: dict) -> dict:
    """返回 {"databases": [{name, tables: [{name, columns: [{name, type}]}]}]}。

    用 COLUMN_TYPE（含精度/长度，如 decimalv3(38,10)、datetime(3)），
    DATA_TYPE 会丢精度（decimal 无 (p,s)，直接建表会报错）。
    """
    import pymysql

    host = conn.get("host") or conn.get("fenodes", "").split(",")[0].split(":")[0] or "localhost"
    c = pymysql.connect(
        host=host,
        port=int(conn.get("port") or conn.get("query_port") or 9030),
        user=conn.get("username") or conn.get("user") or "root",
        password=conn.get("password", ""),
        connect_timeout=5,
        read_timeout=15,
        charset="utf8mb4",
    )
    databases: dict[str, dict[str, list]] = {}
    try:
        with c.cursor() as cur:
            cur.execute(
                """
                SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_TYPE
                FROM information_schema.columns
                WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', '__internal_schema')
                ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """
            )
            for schema, table, column, data_type in cur.fetchall():
                tables = databases.setdefault(schema, {})
                tables.setdefault(table, []).append({"name": column, "type": data_type})
    finally:
        c.close()
    return {
        "databases": [
            {"name": d, "tables": [{"name": t, "columns": cols} for t, cols in tables.items()]}
            for d, tables in databases.items()
        ]
    }
