"""源字段类型 -> SeaTunnel 类型 -> Doris 类型的映射与列名规范化。"""
from __future__ import annotations

import re

# PG: udt_name/data_type -> (SeaTunnel 类型, Doris 类型)
PG_TYPE_MAP: dict[str, tuple[str, str]] = {
    "int2": ("int", "INT"), "int4": ("int", "INT"), "serial": ("int", "INT"), "serial4": ("int", "INT"),
    "int8": ("bigint", "BIGINT"), "bigserial": ("bigint", "BIGINT"), "serial8": ("bigint", "BIGINT"),
    "float4": ("float", "FLOAT"), "float8": ("double", "DOUBLE"),
    "numeric": ("decimal", "DECIMAL(38,10)"), "decimal": ("decimal", "DECIMAL(38,10)"),
    "bool": ("boolean", "BOOLEAN"),
    "text": ("string", "STRING"), "varchar": ("string", "STRING"), "char": ("string", "STRING"),
    "bpchar": ("string", "STRING"), "name": ("string", "STRING"), "uuid": ("string", "STRING"),
    "date": ("date", "DATE"),
    "timestamp": ("timestamp", "DATETIME"), "timestamptz": ("timestamp", "DATETIME"),
    "json": ("string", "VARIANT"), "jsonb": ("string", "VARIANT"),
    "bytea": ("bytes", "STRING"),
    # information_schema data_type 长名称（udt_name 缺失时的兜底）
    "smallint": ("int", "INT"), "integer": ("int", "INT"), "bigint": ("bigint", "BIGINT"),
    "real": ("float", "FLOAT"), "double precision": ("double", "DOUBLE"),
    "boolean": ("boolean", "BOOLEAN"),
    "character varying": ("string", "STRING"), "character": ("string", "STRING"),
    "timestamp without time zone": ("timestamp", "DATETIME"),
    "timestamp with time zone": ("timestamp", "DATETIME"),
}

# MongoDB: bson 类型名 -> (SeaTunnel 类型, Doris 类型)，object/document 走 JSON 序列化
# int32/int64 由 _bson_type 精确区分（BSON 类型级，非值大小猜测），可安全按实际宽度映射。
# document -> string（连接器 convertToString 对 Document 输出 relaxed JSON）；
# array -> array<string>（连接器 createArrayConverter 逐元素转 JSON——若声明 string，
# 连接器会把数组包成 {"_value": [...]}，见 BsonToRowDataConverters/MongoDBConnectorDeserializationSchema）
MONGO_TYPE_MAP: dict[str, tuple[str, str]] = {
    "double": ("double", "DOUBLE"),
    "int": ("int", "INT"), "int32": ("int", "INT"),
    "long": ("bigint", "BIGINT"), "int64": ("bigint", "BIGINT"),
    "bool": ("boolean", "BOOLEAN"),
    "string": ("string", "STRING"), "objectid": ("string", "STRING"),
    "date": ("timestamp", "DATETIME"),
    "decimal128": ("decimal", "DECIMAL(38,10)"),
    "object": ("string", "VARIANT"), "document": ("string", "VARIANT"),
    "array": ("array<string>", "VARIANT"),
}

# kafka 标量 SeaTunnel 类型 -> Doris 类型（嵌套结构单独走 VARIANT/STRING）
ST_SCALAR_TO_DORIS: dict[str, str] = {
    "double": "DOUBLE", "float": "FLOAT", "int": "INT", "bigint": "BIGINT",
    "boolean": "BOOLEAN", "string": "STRING", "bytes": "STRING",
    "date": "DATE", "timestamp": "DATETIME", "decimal": "DECIMAL(38,10)",
}

# Doris -> Doris 同型直通（源 DATA_TYPE 大写即目标类型）
DORIS_PASSTHROUGH = True


def _normalize_col(name: str, used: set[str]) -> str:
    """Doris 列名规范化：小写、非法字符转 _、数字开头加 c_、冲突加 _N 后缀。"""
    col = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower() or "col"
    if col[0].isdigit():
        col = "c_" + col
    base, i = col, 2
    while col in used:
        col = f"{base}_{i}"
        i += 1
    used.add(col)
    return col


def _lookup(table: dict[str, tuple[str, str]], type_name: str) -> tuple[str, str]:
    """类型名查表（忽略大小写与长度修饰），未命中按 STRING 兜底。"""
    key = re.sub(r"\(.*\)", "", (type_name or "").strip().lower())
    return table.get(key, ("string", "STRING"))


def build_mapping(source_type: str, source_columns: list[dict], variant_enabled: bool) -> list[dict]:
    """构建字段映射。

    source_columns: [{"name", "type"}]（PG/Mongo/Doris）或 [{"name", "st_type"}]（kafka proto）。
    返回: [{"source", "st_type", "doris_col", "doris_type", "nested"}]
    """
    mapping: list[dict] = []
    used: set[str] = set()
    for col in source_columns:
        name = col["name"]
        nested = False
        if source_type == "kafka":
            st_type = col["st_type"]
            nested = any(tok in st_type for tok in ("{", "array<", "map<"))
            doris_type = ("VARIANT" if variant_enabled else "STRING") if nested \
                else ST_SCALAR_TO_DORIS.get(st_type, "STRING")
        elif source_type == "postgresql":
            st_type, doris_type = _lookup(PG_TYPE_MAP, col.get("type") or col.get("udt") or "")
            if doris_type == "VARIANT":
                nested = True
                if not variant_enabled:
                    doris_type = "STRING"
            # numeric/decimal 用真实精度（pg_d 已采集 numeric_precision/scale），不再一律 (38,10)
            if st_type == "decimal" and col.get("precision"):
                p = min(int(col["precision"]), 38)
                s = min(int(col.get("scale") or 0), p)
                doris_type = f"DECIMAL({p},{s})"
        elif source_type == "mongodb":
            st_type, doris_type = _lookup(MONGO_TYPE_MAP, col.get("type", ""))
            if doris_type == "VARIANT":
                nested = True
                if not variant_enabled:
                    doris_type = "STRING"
        elif source_type == "doris":
            # COLUMN_TYPE 经 MySQL 协议显示（decimalv3(38,10)/datetime(3)/tinyint(1)），
            # 归一化为规范类型（doris 源 conf 由连接器自行推断 schema，st_type 仅展示用）
            from .doris_ddl import canon_type

            base, params = canon_type(col.get("type", "string"))
            doris_type = f"{base}({params})" if params else base
            st_type = doris_type.lower()
        else:
            raise ValueError(f"不支持的 source_type: {source_type}")
        item = {
            "source": name,
            "st_type": st_type,
            "doris_col": _normalize_col(name, used),
            "doris_type": doris_type,
            "nested": nested,
        }
        # 拍平展开项（kafka 嵌套 message）：透传路径信息，供 SQL transform 与 schema 重建
        for k in ("src_path", "src_root", "src_root_type"):
            if col.get(k):
                item[k] = col[k]
        mapping.append(item)
    return mapping


def mapping_to_schema_fields(mapping: list[dict]) -> list[dict]:
    """提取 [{"name", "st_type"}]，供 conf 模板渲染 schema.fields。

    sink_only 项（如 kafka_ts/etl_time）不进 source schema —— kafka_ts 由 Metadata
    transform 生成，etl_time 走 Doris 列默认值。拍平展开项（src_path）在 schema 中
    以其顶层父字段（src_root，保持嵌套类型）出现，去重后只出现一次。
    """
    fields: list[dict] = []
    seen_roots: set[str] = set()
    for m in mapping:
        if m.get("sink_only"):
            continue
        if m.get("src_path"):
            root = m.get("src_root")
            if root and root not in seen_roots:
                seen_roots.add(root)
                fields.append({"name": root, "st_type": m["src_root_type"]})
            continue
        fields.append({"name": m["source"], "st_type": m["st_type"]})
    return fields


def append_timestamp_columns(mapping: list[dict], source_type: str) -> list[dict]:
    """高级选项 add_timestamps：映射末尾附加时间戳列（原地追加并返回）。

    - kafka_ts（仅 kafka 源）：sink_only，不进 source schema；由 conf 模板生成
      Metadata transform（metadata_fields: EventTime -> kafka_ts）从 kafka 元数据提取，无需补丁
    - etl_time（所有源）：sink_only，不进 source schema，Doris 列 DEFAULT CURRENT_TIMESTAMP 自动填
    """
    if source_type == "kafka":
        if not any(m["doris_col"] == "kafka_ts" for m in mapping):
            mapping.append({
                "source": "kafka_ts", "st_type": "bigint",
                "doris_col": "kafka_ts", "doris_type": "DATETIMEV2(3)",
                "nested": False, "sink_only": True,
                "note": "kafka 消息时间（Metadata 提取 + Doris from_millisecond 转换）",
            })
    if not any(m["doris_col"] == "doris_ts" for m in mapping):
        mapping.append({
            "source": "doris_ts", "st_type": "timestamp",
            "doris_col": "doris_ts", "doris_type": "DATETIMEV2(3)",
            "nested": False, "sink_only": True, "default": "CURRENT_TIMESTAMP(3)",
            "note": "入库时间（Doris 自动填入）",
        })
    return mapping
