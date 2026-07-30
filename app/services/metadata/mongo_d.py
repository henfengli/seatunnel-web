"""MongoDB 元数据发现：库/集合列表 + 采样推断字段 BSON 类型。"""
from __future__ import annotations

from datetime import datetime

_SAMPLE_LIMIT = 20
_SYSTEM_DBS = ("admin", "local", "config")


def _build_uri(conn: dict) -> str:
    """优先使用 conn.uri，否则按 host/port/username/password 拼接（用户名/密码 URL 编码）。"""
    if conn.get("uri"):
        return conn["uri"]
    from urllib.parse import quote_plus

    host = conn.get("host", "localhost")
    port = int(conn.get("port", 27017))
    user = conn.get("username") or conn.get("user")
    if user:
        auth_db = conn.get("auth_db") or conn.get("authSource") or "admin"
        pwd = quote_plus(conn.get("password", ""), safe="")
        return f"mongodb://{quote_plus(user, safe='')}:{pwd}@{host}:{port}/?authSource={auth_db}"
    return f"mongodb://{host}:{port}/"


def _bson_type(value) -> str:
    """Python 值 -> BSON 类型名（精确，不按值大小猜）。

    int32/int64 在 BSON 里是两种类型：pymongo 对 int64 返回 bson.Int64（int 子类，
    必须先于 int 判断），对 int32 返回原生 int。此前的 abs()>=2^31 猜测会在采样值
    全是 0/小值时把 int64 误判成 int32（OrderEvent exchange_time 实际案例）。
    """
    from bson import Decimal128, Int64, ObjectId

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, Int64):
        return "int64"
    if isinstance(value, float):
        return "double"
    if isinstance(value, int):
        return "int32"
    if isinstance(value, str):
        return "string"
    if isinstance(value, datetime):
        return "date"
    if isinstance(value, dict):
        return "document"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, Decimal128):
        return "decimal128"
    if isinstance(value, ObjectId):
        return "objectId"
    if isinstance(value, bytes):
        return "binData"
    return type(value).__name__


_INT_RANK = {"int32": 1, "int64": 2, "double": 3}


def _sample_fields(collection) -> list[dict]:
    """最多采样 _SAMPLE_LIMIT 条文档合并字段，推断 BSON 类型名。

    拓宽只在数值家族内生效（int32 < int64 < double），整数/小数混存时取更宽者；
    不会用 int 覆盖 double/string（整数值与小数值混存在业务集合很常见）。
    """
    fields: dict[str, str] = {}
    for doc in collection.find().limit(_SAMPLE_LIMIT):
        for name, value in doc.items():
            t = _bson_type(value)
            if name not in fields or (fields[name] == "null" and t != "null"):
                fields[name] = t
            elif fields[name] in _INT_RANK and t in _INT_RANK \
                    and _INT_RANK[t] > _INT_RANK[fields[name]]:
                fields[name] = t
    return [{"name": n, "type": t} for n, t in fields.items()]


# $jsonSchema validator 的 bsonType -> 内部类型名
_VALIDATOR_TYPES = {
    "double": "double", "string": "string", "objectId": "objectId", "bool": "bool",
    "date": "date", "int": "int32", "long": "int64", "decimal": "decimal128",
    "object": "document", "array": "array",
}


def _validator_fields(database, coll_name: str) -> dict[str, str]:
    """读集合 $jsonSchema validator 的声明字段类型（没有 validator 返回 {}）。

    validator 是 MongoDB 的声明式 schema，类型精确，优先于采样推断。
    """
    try:
        res = database.command("listCollections", filter={"name": coll_name})
        for info in res.get("cursor", {}).get("firstBatch", []):
            schema = ((info.get("options") or {}).get("validator") or {}).get("$jsonSchema") or {}
            out: dict[str, str] = {}
            for name, spec in (schema.get("properties") or {}).items():
                bt = (spec or {}).get("bsonType")
                if isinstance(bt, list):
                    bt = next((x for x in bt if x != "null"), None)
                if bt in _VALIDATOR_TYPES:
                    out[name] = _VALIDATOR_TYPES[bt]
            return out
    except Exception:  # noqa: BLE001 - 老版本/无权限时退回纯采样
        pass
    return {}


def discover(conn: dict) -> dict:
    """返回 {"databases": [{name, collections: [{name, fields: [{name, type}]}]}]}。

    字段类型来源：$jsonSchema validator（精确，优先）+ 采样推断（BSON 类型级精确，兜底）。
    """
    from pymongo import MongoClient

    client = MongoClient(_build_uri(conn), serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    try:
        databases = []
        for db_name in client.list_database_names():
            if db_name in _SYSTEM_DBS:
                continue
            collections = []
            for coll_name in client[db_name].list_collection_names():
                if coll_name.startswith("system."):
                    continue
                merged: dict[str, str] = {f["name"]: f["type"]
                                          for f in _sample_fields(client[db_name][coll_name])}
                merged.update(_validator_fields(client[db_name], coll_name))
                collections.append({
                    "name": coll_name,
                    "fields": [{"name": n, "type": t} for n, t in merged.items()],
                })
            databases.append({"name": db_name, "collections": collections})
    finally:
        client.close()
    return {"databases": databases}
