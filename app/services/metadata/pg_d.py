"""PostgreSQL 元数据发现：schema/表/列（information_schema）。"""
from __future__ import annotations

_EXCLUDED = ("pg_catalog", "information_schema")


def discover(conn: dict) -> dict:
    """返回 {"schemas": [{name, tables: [{name, columns: [{name, type, udt}]}]}]}。"""
    import psycopg2

    conn_kwargs = dict(
        host=conn.get("host", "localhost"),
        port=int(conn.get("port", 5432)),
        dbname=conn.get("db") or conn.get("database"),
        user=conn.get("username") or conn.get("user"),
        password=conn.get("password", ""),
        connect_timeout=5,
    )
    schemas: dict[str, dict[str, list]] = {}
    with psycopg2.connect(**conn_kwargs) as c:
        with c.cursor() as cur:
            cur.execute("SET statement_timeout = 10000")
            cur.execute(
                """
                SELECT table_schema, table_name, column_name, data_type, udt_name,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            for schema, table, column, data_type, udt_name, precision, scale in cur.fetchall():
                tables = schemas.setdefault(schema, {})
                col: dict = {"name": column, "type": data_type, "udt": udt_name}
                if precision is not None:
                    col["precision"] = precision
                if scale is not None:
                    col["scale"] = scale
                tables.setdefault(table, []).append(col)
    return {
        "schemas": [
            {"name": s, "tables": [{"name": t, "columns": cols} for t, cols in tables.items()]}
            for s, tables in schemas.items()
        ]
    }
