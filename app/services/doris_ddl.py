"""Doris 自动建表与字段演进（ADD COLUMN），通过 MySQL 协议连 query_port。

连接信息由调用方以 env dict（见 services.envs.to_dict 的 "doris" 段）传入。
"""
from __future__ import annotations

import re
import time


def _connect(doris: dict):
    """按环境 doris 配置建立 pymysql 连接（取第一个 fenode 的 host）。"""
    import pymysql

    host = doris["fenodes"].split(",")[0].split(":")[0]
    return pymysql.connect(
        host=host,
        port=int(doris.get("query_port", 9030)),
        user=doris.get("username", "root"),
        password=doris.get("password", ""),
        connect_timeout=5,
        read_timeout=30,
        charset="utf8mb4",
        autocommit=True,
    )


def _column_type(col: dict, variant_enabled: bool) -> str:
    """nested 列按 variant 开关降级为 STRING。"""
    if col.get("nested"):
        return "VARIANT" if variant_enabled else "STRING"
    return col["doris_type"]


def _column_def(col: dict, variant_enabled: bool, model: str = "DUPLICATE",
                is_key: bool = False) -> str:
    """列定义：类型 + AGGREGATE 模型的聚合函数（非 key 列）+ 可选 DEFAULT 子句。

    key 列的 STRING 自动转 VARCHAR(512)——Doris 不允许 STRING 类型做 key 列
    （errCode 1105: String Type should not be used in key column）。
    """
    col_type = _column_type(col, variant_enabled)
    if is_key and col_type == "STRING":
        col_type = "VARCHAR(512)"
    ddl = f"`{col['doris_col']}` {col_type}"
    if model == "AGGREGATE" and not is_key:
        ddl += f" {col.get('agg', 'REPLACE')}"
    if col.get("default"):
        ddl += f" DEFAULT {col['default']}"
    return ddl


def key_columns(mapping: list[dict], ttl: dict | None = None) -> list[str]:
    """key 列规则：有 is_key 标记按标记（mapping 顺序，不截断——UNIQUE/AGG 的 key 是完整去重语义）；
    否则前 3 个非 nested 标量列（DUPLICATE 启发式）；ttl 列最前（Doris 要求分区列在 key 中）。"""
    marked = [m["doris_col"] for m in mapping if m.get("is_key")]
    if marked:
        key_cols = marked
    else:
        scalar_cols = [m["doris_col"] for m in mapping if not m.get("nested")]
        key_cols = scalar_cols[:3] if scalar_cols else [m["doris_col"] for m in mapping]
    if ttl:
        rest = [c for c in key_cols if c != ttl["column"]]
        # 截断只适用于无标记的启发式路径；用户显式标记的 key 一个都不能丢
        key_cols = [ttl["column"]] + (rest if marked else rest[:2])
    return key_cols


def _ttl_parts(ttl: dict) -> tuple[int, str, str]:
    """ttl dict 兼容两种结构：新 {"num","unit","column"} / 老 {"days","column"}（按 DAY）。"""
    return int(ttl.get("num") or ttl.get("days")), ttl.get("unit", "DAY"), ttl["column"]


def build_create_table(
    db_name: str,
    table: str,
    mapping: list[dict],
    variant_enabled: bool,
    buckets: int,
    replication_num: int = 3,
    ttl: dict | None = None,
    model: str = "DUPLICATE",
) -> str:
    """生成建表语句：key 列按 is_key 标记/默认规则，按第一列 HASH 分桶。

    replication_num：副本数。单机 Doris（1 BE）必须为 1，否则副本无法分配、表不健康。
    model：DUPLICATE（默认，日志追加）/ UNIQUE（幂等去重）/ AGGREGATE（预聚合，非 key 列带聚合函数）。
    ttl（{"num","unit","column"}）非空时：RANGE 动态分区按 unit 粒度留存 num，
    分区列必须为 DATE/DATETIME 且放 KEY 最前（Doris 要求分区列在 key 中）。
    """
    model = (model or "DUPLICATE").upper()
    ttl_num = ttl_unit = ttl_col = None
    if ttl:
        ttl_num, ttl_unit, ttl_col = _ttl_parts(ttl)
        if ttl_unit not in ("HOUR", "DAY", "WEEK", "MONTH", "YEAR"):
            raise ValueError(f"Doris 动态分区不支持 {ttl_unit} 粒度（仅 HOUR/DAY/WEEK/MONTH/YEAR）")
    key_cols = key_columns(mapping, {"column": ttl_col} if ttl_col else None)
    if model in ("UNIQUE", "AGGREGATE") and not key_cols:
        raise ValueError(f"{model} 模型 key 列不能为空（字段映射为空？）")
    key_set = set(key_cols)
    # Doris 要求 key 列是表结构的有序前缀：列定义物理重排，key 列按 key 顺序置顶
    ordered = ([m for c in key_cols for m in mapping if m["doris_col"] == c]
               + [m for m in mapping if m["doris_col"] not in key_set])
    cols = ", ".join(
        _column_def(m, variant_enabled, model, m["doris_col"] in key_set) for m in ordered)
    keys = ", ".join(f"`{c}`" for c in key_cols)
    first = key_cols[0] if key_cols else mapping[0]["doris_col"]  # 分桶列取首个 key 列
    sql = f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table}` (\n  {cols}\n)\n"
    sql += f"{model} KEY({keys})\n"
    if ttl:
        sql += f"PARTITION BY RANGE(`{ttl_col}`)()\n"
    sql += f"DISTRIBUTED BY HASH(`{first}`) BUCKETS {buckets}\n"
    props = [f'"replication_num" = "{replication_num}"']
    if ttl:
        props += [
            '"dynamic_partition.enable" = "true"',
            f'"dynamic_partition.time_unit" = "{ttl_unit}"',
            f'"dynamic_partition.start" = "-{ttl_num}"',
            '"dynamic_partition.end" = "3"',
            '"dynamic_partition.prefix" = "p"',
            f'"dynamic_partition.buckets" = "{buckets}"',
        ]
        # 预建历史分区：全量/存量同步的历史数据落在 start~end 窗口外会被整批拒绝
        if ttl.get("history_num"):
            props += [
                '"dynamic_partition.create_history_partition" = "true"',
                f'"dynamic_partition.history_partition_num" = "{ttl["history_num"]}"',
            ]
    sql += "PROPERTIES (\n  " + ",\n  ".join(props) + "\n)"
    return sql


def _existing_columns(cur, db_name: str, table: str) -> list[str] | None:
    """查现有列名；表不存在返回 None。"""
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (db_name, table),
    )
    rows = cur.fetchall()
    return [r[0] for r in rows] if rows else None


def _existing_column_types(cur, db_name: str, table: str) -> dict[str, str] | None:
    """查现有列名 -> 类型（COLUMN_TYPE 大写）；表不存在返回 None。"""
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (db_name, table),
    )
    rows = cur.fetchall()
    return {r[0]: r[1].upper() for r in rows} if rows else None


def _existing_column_defaults(cur, db_name: str, table: str) -> dict[str, str]:
    """查现有列的默认值（仅非 NULL 项）；MODIFY COLUMN 时重述，防止默认值被静默清掉。"""
    cur.execute(
        "SELECT COLUMN_NAME, COLUMN_DEFAULT FROM information_schema.columns "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_DEFAULT IS NOT NULL",
        (db_name, table),
    )
    return {r[0].lower(): str(r[1]) for r in cur.fetchall()}


def _default_clause(raw: str) -> str:
    """information_schema 的 COLUMN_DEFAULT 原文 -> DEFAULT 子句（引号格式兼容）。"""
    v = raw.strip()
    if re.match(r"^(CURRENT_TIMESTAMP(\(\d+\))?|-?\d+(\.\d+)?|'.*')$", v, re.IGNORECASE):
        return f" DEFAULT {v}"
    return f" DEFAULT '{v.replace(chr(39), chr(39) * 2)}'"


def _parse_ttl_props(text: str) -> dict | None:
    """从 SHOW CREATE TABLE 文本解析动态分区配置；未启用返回 None。"""
    if not re.search(r'"dynamic_partition\.enable"\s*=\s*"true"', text, re.IGNORECASE):
        return None
    unit = re.search(r'"dynamic_partition\.time_unit"\s*=\s*"(\w+)"', text, re.IGNORECASE)
    start = re.search(r'"dynamic_partition\.start"\s*=\s*"(-?\d+)"', text)
    chp = re.search(r'"dynamic_partition\.create_history_partition"\s*=\s*"(\w+)"',
                    text, re.IGNORECASE)
    hpn = re.search(r'"dynamic_partition\.history_partition_num"\s*=\s*"(-?\d+)"', text)
    return {"unit": unit.group(1).upper() if unit else "",
            "start": int(start.group(1)) if start else None,
            "create_history": chp.group(1).lower() == "true" if chp else False,
            "history_num": int(hpn.group(1)) if hpn else None}


def _parse_show_create(text: str) -> dict:
    """解析 SHOW CREATE TABLE：表模型、key 列、是否分区、动态分区配置、分桶数。"""
    model = "DUPLICATE"
    key_cols: list[str] = []
    m = re.search(r"\b(DUPLICATE|UNIQUE|AGGREGATE)\s+KEY\s*\(([^)]*)\)", text, re.IGNORECASE)
    if m:
        model = m.group(1).upper()
        key_cols = [c.strip().strip("`") for c in m.group(2).split(",") if c.strip()]
    buckets = None
    b = re.search(r"\bBUCKETS\s+(\d+)", text, re.IGNORECASE)
    if b:
        buckets = int(b.group(1))
    return {
        "model": model,
        "key_cols": key_cols,
        "partitioned": bool(re.search(r"\bPARTITION\s+BY\s+", text, re.IGNORECASE)),
        "ttl": _parse_ttl_props(text),
        "buckets": buckets,
    }


def _base_type(t: str) -> tuple[str, str]:
    """'DATETIMEV2(3)' -> ('DATETIMEV2', '3')；无法解析返回 (原文大写, '')。"""
    m = re.match(r"^\s*([A-Za-z0-9]+)\s*(?:\(([^)]*)\))?\s*$", str(t or ""))
    return (m.group(1).upper(), m.group(2) or "") if m else (str(t).upper(), "")


def canon_type(t: str) -> tuple[str, str]:
    """MySQL 协议显示名 -> Doris 规范类型名（返回 (基类型, 参数)）。

    依据 Doris BE schema_columns_scanner.cpp 的 COLUMN_TYPE 输出规则归一化：
    - BOOLEAN 报 tinyint(1)；整型带显示宽度（int(11)/bigint(20) 等，无类型含义）；
    - DATETIMEV2/DATEV2 报 datetime(p)/date（旧名）；STRING 报 string 或 text（版本差异）；
    - DECIMAL 各档报 decimal(p,s)/decimalv3(p,s)；JSONB 报 json。
    """
    base, params = _base_type(t)
    if base == "TINYINT" and params == "1":
        return "BOOLEAN", ""
    if base in ("TINYINT", "SMALLINT", "INT", "BIGINT"):
        return base, ""  # 显示宽度 int(11) 无意义
    if base in ("DECIMALV2", "DECIMALV3"):
        return "DECIMAL", params
    if base == "TEXT":
        return "STRING", ""
    if base == "JSON":
        return "JSONB", ""
    if base == "DATETIME":
        return "DATETIMEV2", params or "0"
    if base == "DATE":
        return "DATEV2", ""
    return base, params


# 可在线 MODIFY COLUMN 的类型转换（Doris ColumnType.schemaChangeMatrix 的保守子集，只保留源码确认项）
_TYPE_WIDEN = {
    ("INT", "BIGINT"), ("INT", "LARGEINT"), ("BIGINT", "LARGEINT"),
    ("FLOAT", "DOUBLE"),
    ("DATEV2", "DATETIMEV2"), ("DATETIMEV2", "DATEV2"),
}


def _type_change_level(old: str, new: str) -> str:
    """现有类型 -> 目标类型：same（一致）/ online（可在线 MODIFY）/ recreate（只能重建）。"""
    ob, op = canon_type(old)
    nb, np_ = canon_type(new)
    if (ob, op) == (nb, np_):
        return "same"
    if ob == nb:
        if ob == "VARCHAR":
            try:
                return "online" if (not op or not np_ or int(np_) >= int(op)) else "recreate"
            except ValueError:
                return "recreate"
        return "recreate"  # 同族参数变化（精度/长度收窄等），保守按重建
    # 官方转换清单：除 DATE/DATETIME 外都可转 STRING；STRING 不能转任何其他类型
    if (ob, nb) in _TYPE_WIDEN or (nb == "STRING" and ob not in ("DATEV2", "DATETIMEV2")):
        return "online"
    return "recreate"


def _effective_type(col: dict, variant_enabled: bool, is_key: bool) -> str:
    """映射项的实际建表类型（nested 降级 + key 列 STRING->VARCHAR(512)）。"""
    t = _column_type(col, variant_enabled)
    if is_key and t == "STRING":
        t = "VARCHAR(512)"
    return t


def compat_decision(old_cols: dict | None, show: dict | None, mapping: list[dict],
                    ttl: dict | None, model: str, buckets: int | None,
                    variant_enabled: bool = True) -> dict:
    """纯函数：现有表 vs 目标配置的兼容性判定。

    old_cols None = 表不存在（level=none）。
    level: none/same/online/recreate；online=可在线执行的动作列表；reasons=必须重建的原因。
    """
    res: dict = {"level": "none", "online": [], "reasons": [], "notes": [],
                 "added": [], "type_changes": [], "dropped": [], "ttl_alter": None}
    if old_cols is None:
        return res
    res["level"] = "same"
    model = (model or "DUPLICATE").upper()
    show = show or {}
    old_model = show.get("model", "DUPLICATE")
    if old_model != model:
        res["reasons"].append(f"表模型不一致：现有 {old_model}，当前配置 {model}")

    ttl_num = ttl_unit = None
    if ttl:
        ttl_num, ttl_unit, ttl_col = _ttl_parts(ttl)
    desired_keys = key_columns(mapping, {"column": ttl["column"]} if ttl else None)
    # SHOW CREATE 的 key 列统一小写再比对（映射列名已规范化小写，外部建的表可能大写）
    old_keys = [k.lower() for k in show.get("key_cols", [])]
    if model in ("UNIQUE", "AGGREGATE") and set(desired_keys) != set(old_keys):
        res["reasons"].append(f"key 列不一致：现有 {old_keys}，当前配置 {desired_keys}")

    if ttl:
        if not show.get("partitioned"):
            res["reasons"].append("现有表未分区，无法在线启用 TTL 动态分区（PARTITION BY 是建表子句）")
        else:
            cur = show.get("ttl")
            desired_hn = ttl.get("history_num")
            history_mismatch = bool(desired_hn) and (
                not cur or not cur.get("create_history")
                or cur.get("history_num") != desired_hn)
            if not cur or cur["start"] != -ttl_num or cur["unit"] != ttl_unit \
                    or history_mismatch:
                res["ttl_alter"] = {"num": ttl_num, "unit": ttl_unit,
                                    "history_num": desired_hn}
                res["online"].append(f"在线更新动态分区配置（TTL 留存 {ttl_num} {ttl_unit}"
                                     + (f"，预建历史分区 {desired_hn}" if desired_hn else "") + "）")

    key_set = set(desired_keys)
    old_lookup = {k.lower(): v for k, v in old_cols.items()}
    for m in mapping:
        col = m["doris_col"]
        eff = _effective_type(m, variant_enabled, col in key_set)
        old_t = old_lookup.get(col.lower())
        if old_t is None:
            res["added"].append(col)
            res["online"].append(f"ADD COLUMN `{col}` {eff}")
            continue
        lvl = _type_change_level(old_t, eff)
        # 分区列/分桶列（首 key 列）Doris 禁止任何修改，类型有差异一律判重建
        if lvl != "same" and (col == desired_keys[0] or (ttl and col == ttl["column"])):
            res["reasons"].append(
                f"列 {col} 是{'分区' if ttl and col == ttl['column'] else '分桶'}列，"
                f"Doris 不允许修改（现有 {old_t}，需要 {eff}）")
        elif lvl == "online":
            res["type_changes"].append((col, old_t, eff))
            res["online"].append(f"MODIFY COLUMN `{col}` {old_t} -> {eff}")
        elif lvl == "recreate":
            res["reasons"].append(f"列 {col} 类型不可在线转换：现有 {old_t}，需要 {eff}")

    new_set = {m["doris_col"].lower() for m in mapping}
    res["dropped"] = [c for c in old_cols if c.lower() not in new_set]
    if res["dropped"]:
        res["notes"].append(f"旧表列 {res['dropped']} 不在当前映射中（保留不删，迁移重建时会被丢弃）")
    if buckets and show.get("buckets") and buckets != show["buckets"]:
        res["notes"].append(f"分桶数不一致（现有 {show['buckets']}，配置 {buckets}；已有分区不变）")

    if res["reasons"]:
        res["level"] = "recreate"
    elif res["online"]:
        res["level"] = "online"
    return res


def check_compat(doris: dict, db_name: str, table: str, mapping: list[dict],
                 ttl: dict | None = None, model: str = "DUPLICATE",
                 buckets: int | None = None) -> dict:
    """读目标表现状（information_schema + SHOW CREATE TABLE）并做兼容性判定。"""
    c = _connect(doris)
    try:
        with c.cursor() as cur:
            cols = _existing_column_types(cur, db_name, table)
            text = ""
            defaults: dict[str, str] = {}
            if cols is not None:
                cur.execute(f"SHOW CREATE TABLE `{db_name}`.`{table}`")
                row = cur.fetchone()
                text = row[1] if row and len(row) > 1 else ""
                defaults = _existing_column_defaults(cur, db_name, table)
    finally:
        c.close()
    show = _parse_show_create(text) if text else None
    r = compat_decision(cols, show, mapping, ttl, model, buckets,
                        bool(doris.get("variant_enabled", True)))
    r["exists"] = cols is not None
    r["old_cols"] = cols
    r["show"] = show
    r["defaults"] = defaults
    return r


def ensure_table(doris: dict, db_name: str, table: str, mapping: list[dict],
                 ttl: dict | None = None, model: str = "DUPLICATE",
                 buckets: int | None = None, dry_run: bool = False) -> dict:
    """确保库/表/列与配置一致，返回 {"created","added_columns","ddl","compat"}。

    兼容性分级（check_compat）：
    - 表不存在 -> 建表；
    - online -> 在线演进：ADD COLUMN / MODIFY COLUMN（可转换类型）/ 动态分区 ALTER（TTL 窗口）；
    - recreate -> 不执行任何 DDL，返回 needs_recreate + reasons，由上层拒绝并提示。
    dry_run=True 只做检查不执行（更新编排在停作业前预检用）。
    """
    compat = check_compat(doris, db_name, table, mapping, ttl, model, buckets)
    result = {
        "created": not compat["exists"],
        "added_columns": [],
        "ddl": "",
        "compat": compat,
        "online_actions": compat["online"],
    }
    if compat["level"] == "recreate":
        result["needs_recreate"] = True
        result["reasons"] = compat["reasons"]
        return result
    if dry_run:
        return result

    variant_enabled = bool(doris.get("variant_enabled", True))
    buckets = buckets or int(doris.get("default_buckets", 10))
    replication_num = int(doris.get("replication_num", 1))
    model = (model or "DUPLICATE").upper()
    stmts = [f"CREATE DATABASE IF NOT EXISTS `{db_name}`"]
    if not compat["exists"]:
        stmts.append(build_create_table(db_name, table, mapping, variant_enabled,
                                        buckets, replication_num, ttl, model))
    else:
        key_cols = key_columns(mapping, {"column": ttl["column"]} if ttl else None)
        key_set = set(key_cols)
        mapping_by_col = {m["doris_col"]: m for m in mapping}
        for col in compat["added"]:
            m = mapping_by_col[col]
            stmts.append(
                f"ALTER TABLE `{db_name}`.`{table}` ADD COLUMN "
                f"{_column_def(m, variant_enabled, model, col in key_set)}")
        for col, _old_t, new_t in compat["type_changes"]:
            # MODIFY COLUMN 是异步 schema change（提交即返回）；
            # key 列需带 KEY 关键字；AGGREGATE 模型的 value 列必须带聚合函数（Doris 文档要求）；
            # 原列 DEFAULT 必须重述（Doris 文档：MODIFY 需声明完整列信息，否则默认值被清掉）
            kw = ""
            if col in key_set:
                kw = " KEY"
            elif model == "AGGREGATE":
                kw = f" {mapping_by_col[col].get('agg', 'REPLACE')}"
            default_raw = (compat.get("defaults") or {}).get(col.lower())
            if default_raw is not None:
                kw += _default_clause(default_raw)
            stmts.append(f"ALTER TABLE `{db_name}`.`{table}` MODIFY COLUMN `{col}` {new_t}{kw}")
        if compat.get("ttl_alter"):
            ta = compat["ttl_alter"]
            props = (f'"dynamic_partition.enable" = "true", '
                     f'"dynamic_partition.time_unit" = "{ta["unit"]}", '
                     f'"dynamic_partition.start" = "-{ta["num"]}", '
                     f'"dynamic_partition.end" = "3", '
                     f'"dynamic_partition.prefix" = "p"')
            if ta.get("history_num"):
                props += (f', "dynamic_partition.create_history_partition" = "true", '
                          f'"dynamic_partition.history_partition_num" = "{ta["history_num"]}"')
                # 同步建历史分区：动态分区调度器要等下一个检查周期（默认 600s）才按
                # history_partition_num 建分区，这里关开关立即建（IF NOT EXISTS 与调度器
                # 产物一致），存量同步不用再等
                stmts.append(f'ALTER TABLE `{db_name}`.`{table}` SET '
                             f'("dynamic_partition.enable" = "false")')
                cap = 366 * 24 if ta["unit"] == "HOUR" else 366
                for name, lo, hi in _history_partition_names(
                        ta["unit"], min(int(ta["history_num"]), cap)):
                    stmts.append(
                        f"ALTER TABLE `{db_name}`.`{table}` ADD PARTITION IF NOT EXISTS "
                        f"`{name}` VALUES [('{lo}'), ('{hi}'))")
            stmts.append(f'ALTER TABLE `{db_name}`.`{table}` SET ({props})')
    c = _connect(doris)
    try:
        result["notes"] = _exec_all(c, stmts, tolerate_noop=True)
    finally:
        c.close()
    result["ddl"] = ";\n".join(stmts) + ";"
    result["added_columns"] = [s for s in stmts if s.startswith("ALTER TABLE")]
    if compat.get("ttl_alter"):
        result["ttl_altered"] = True
    elif ttl and compat["exists"]:
        result["ttl_active"] = True  # 已存在表的动态分区与作业 TTL 一致（如刚重建过）
    return result


def _exec_all(c, stmts: list[str], tolerate_noop: bool = False,
              max_wait_sec: int = 300) -> list[str]:
    """逐条执行 DDL；返回被跳过的幂等语句备注。

    - tolerate_noop：Doris 报 "Nothing is changed"（目标状态已生效的幂等重放）时跳过不报错；
    - 表处于 SCHEMA_CHANGE 状态（MODIFY COLUMN 等异步 schema change 未完成期间 Doris 拒绝
      新的 ALTER ops）时轮询等待重试，max_wait_sec 超时后带指引抛出；
    - 其他异常附 SQL 上下文抛出，便于定位是哪一条失败。
    """
    notes: list[str] = []
    with c.cursor() as cur:
        for sql in stmts:
            waited = 0
            while True:
                try:
                    cur.execute(sql)
                    break
                except Exception as e:  # noqa: BLE001
                    msg = str(e)
                    if tolerate_noop and "Nothing is changed" in msg:
                        notes.append(f"已是目标状态，跳过: {sql[:120]}")
                        break
                    if ("is not NORMAL" in msg or "SCHEMA_CHANGE" in msg) \
                            and waited < max_wait_sec:
                        time.sleep(2)
                        waited += 2
                        continue
                    if "is not NORMAL" in msg or "SCHEMA_CHANGE" in msg:
                        raise RuntimeError(
                            f"{e}（等待表 schema change 完成超时，请在 Doris 中执行 "
                            f"SHOW ALTER TABLE COLUMN 确认任务结束后重试）"
                            f" [SQL: {sql[:200]}]") from e
                    raise RuntimeError(f"{e} [SQL: {sql[:300]}]") from e
    return notes


def epoch_expr(col: str) -> str:
    """迁移 SQL（普通 Doris SQL，CASE 可用）里 epoch 整数列 -> from_millisecond 表达式。

    毫秒 ~1e12 / 微秒 ~1e15 / 纳秒 ~1e18：按阈值逐级除到毫秒，混合格式兼容。
    注意：stream load 的 columns 映射头不放复杂表达式（量级自适应在 SeaTunnel
    SQL transform 里做，见 render._sql_select），这里仅用于迁移 INSERT SELECT 与补分区。
    """
    c = f"`{col}`"
    return (f"from_millisecond(CASE WHEN {c} >= 100000000000000000 THEN {c} / 1000000 "
            f"WHEN {c} >= 100000000000000 THEN {c} / 1000 ELSE {c} END)")


_LITERAL_RE = re.compile(r"^(NULL|CURRENT_TIMESTAMP(\(3\))?|-?\d+(\.\d+)?|'([^']|'')*')$",
                         re.IGNORECASE)


def build_migration_plan(old_cols: dict, mapping: list[dict],
                         variant_enabled: bool, desired_keys: list[str]) -> tuple[list[dict], list[str]]:
    """数据迁移逐列计划（纯函数）。

    每列 kind：
    - same：类型一致，直接迁移；
    - cast_safe：可安全转换（类型放大等），自动 CAST；
    - cast_risky：收窄/不可直接转换，用户决定 强制CAST 或 填常量；
    - missing：旧表没有该列，用户可填常量（缺省用映射 default，再缺省 NULL）。
    dropped：旧表有、新映射没有的列（迁移时被丢弃）。
    """
    key_set = set(desired_keys)
    old_lookup = {k.lower(): v for k, v in (old_cols or {}).items()}
    plan: list[dict] = []
    for m in mapping:
        col = m["doris_col"]
        eff = _effective_type(m, variant_enabled, col in key_set)
        old_t = old_lookup.get(col.lower())
        if old_t is None:
            plan.append({"col": col, "type": eff, "kind": "missing",
                         "default": m.get("default", "")})
            continue
        # BIGINT epoch 整数 -> DATETIMEV2(3)（ms_epoch 列）：CAST 语义不对，必须 from_millisecond
        if m.get("ms_epoch") and _base_type(old_t)[0] in ("BIGINT", "INT") \
                and eff.startswith("DATETIME"):
            plan.append({"col": col, "type": eff, "old_type": old_t, "kind": "ms_epoch"})
            continue
        lvl = _type_change_level(old_t, eff)
        kind = {"same": "same", "online": "cast_safe"}.get(lvl, "cast_risky")
        plan.append({"col": col, "type": eff, "old_type": old_t, "kind": kind})
    new_set = {m["doris_col"].lower() for m in mapping}
    dropped = [c for c in (old_cols or {}) if c.lower() not in new_set]
    return plan, dropped


def build_select_exprs(plan: list[dict], decisions: dict) -> tuple[list[str], list[str]]:
    """按迁移计划 + 用户决策生成 INSERT SELECT 的列表达式；返回 (exprs, 错误列表)。"""
    exprs: list[str] = []
    errs: list[str] = []
    for p in plan:
        col = p["col"]
        if p["kind"] == "ms_epoch":
            exprs.append(f"{epoch_expr(col)} AS `{col}`")
        elif p["kind"] == "missing":
            v = (decisions.get(f"fill_{col}") or "").strip() or p.get("default") or "NULL"
            if not _LITERAL_RE.match(v):
                errs.append(f"列 {col} 的填充值非法（只允许 数字 / '字符串' / NULL / CURRENT_TIMESTAMP）")
                continue
            exprs.append(f"{v} AS `{col}`")
        elif p["kind"] == "cast_risky":
            if decisions.get(f"conv_{col}", "cast") == "const":
                v = (decisions.get(f"fillv_{col}") or "").strip() or "NULL"
                if not _LITERAL_RE.match(v):
                    errs.append(f"列 {col} 的常量值非法（只允许 数字 / '字符串' / NULL）")
                    continue
                exprs.append(f"{v} AS `{col}`")
            else:
                exprs.append(f"CAST(`{col}` AS {p['type']}) AS `{col}`")
        elif p["kind"] == "cast_safe":
            exprs.append(f"CAST(`{col}` AS {p['type']}) AS `{col}`")
        else:
            exprs.append(f"`{col}`")
    return exprs, errs


def _partition_span(d, unit: str) -> tuple[str, str, str]:
    """date_trunc 出的分区点 -> (分区名, 下界, 上界)。unit = DAY/HOUR/WEEK/MONTH。"""
    from datetime import date as _date
    from datetime import timedelta

    unit = unit.upper()
    if unit == "DAY":
        return f"p{d:%Y%m%d}", f"{d:%Y-%m-%d}", f"{d + timedelta(days=1):%Y-%m-%d}"
    if unit == "HOUR":
        hi = d + timedelta(hours=1)
        return f"p{d:%Y%m%d%H}", f"{d:%Y-%m-%d %H:%M:%S}", f"{hi:%Y-%m-%d %H:%M:%S}"
    if unit == "WEEK":
        return f"p{d:%Y%m%d}", f"{d:%Y-%m-%d}", f"{d + timedelta(days=7):%Y-%m-%d}"
    if unit == "MONTH":
        lo = d.replace(day=1)
        hi = _date(d.year + (1 if d.month == 12 else 0), d.month % 12 + 1, 1)
        return f"p{lo:%Y%m}", f"{lo:%Y-%m-%d}", f"{hi:%Y-%m-%d}"
    raise RuntimeError(f"数据迁移暂不支持 {unit} 粒度的分区补建")


def _history_partition_names(unit: str, count: int) -> list[tuple[str, str, str]]:
    """从当前时间往前 count 个分区点的 (分区名, 下界, 上界)（用于 ALTER 时同步建历史分区）。

    与动态分区调度器会建的完全一致，但立即生效——不等 dynamic_partition_check_interval_seconds。
    """
    from datetime import datetime, timedelta

    now = datetime.now()
    unit = unit.upper()
    if unit == "DAY":
        base = now.replace(hour=0, minute=0, second=0, microsecond=0)
        step = timedelta(days=1)
    elif unit == "HOUR":
        base = now.replace(minute=0, second=0, microsecond=0)
        step = timedelta(hours=1)
    elif unit == "WEEK":
        base = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
        step = timedelta(days=7)
    elif unit == "MONTH":
        base = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        step = None
    else:
        return []
    out = []
    for i in range(count):
        if step is None:  # MONTH
            m = base.month - i
            y = base.year + (m - 1) // 12
            mm = (m - 1) % 12 + 1
            d = datetime(y, mm, 1)
        else:
            d = base - i * step
        out.append(_partition_span(d, unit))
    return out


def _provision_partitions(cur, db_name: str, table: str, tmp: str,
                          ttl_col: str, ttl_unit: str, ms_epoch: bool = False) -> list[str]:
    """按 tmp 表实际数据补建历史分区。

    动态分区只自动创建窗口（start~end）内的分区，窗口外的历史数据没有分区可落，
    INSERT 会报 no partition for this tuple（strict 模式整批失败）。
    ms_epoch=True 时旧表该列是 epoch 整数（毫秒/微秒/纳秒按量级自适应）。
    """
    if ms_epoch:
        col_expr = epoch_expr(ttl_col)
    else:
        col_expr = f"`{ttl_col}`"
    cur.execute(
        f"SELECT DISTINCT date_trunc({col_expr}, '{ttl_unit.lower()}') "
        f"FROM `{db_name}`.`{tmp}` WHERE `{ttl_col}` IS NOT NULL")
    points = sorted(r[0] for r in cur.fetchall() if r[0] is not None)
    cap = 366 * 24 if ttl_unit.upper() == "HOUR" else 366
    if len(points) > cap:
        raise RuntimeError(
            f"待迁移数据横跨 {len(points)} 个 {ttl_unit} 分区（>{cap}），请先清理历史数据再迁移")
    created = []
    for d in points:
        name, lo, hi = _partition_span(d, ttl_unit)
        cur.execute(
            f"ALTER TABLE `{db_name}`.`{table}` ADD PARTITION IF NOT EXISTS `{name}` "
            f"VALUES [('{lo}'), ('{hi}'))")
        created.append(name)
    return created


def migrate_table(doris: dict, db_name: str, table: str, mapping: list[dict],
                  ttl: dict | None, model: str, buckets: int | None,
                  select_exprs: list[str]) -> dict:
    """数据迁移重建：RENAME 原表为 tmp_ -> 建新表 -> （按需补建历史分区）-> INSERT SELECT
    迁移 -> 行数核对 -> 删 tmp。

    任一步失败回滚（删新表、tmp 改回原名）后抛异常；行数不一致（非 UNIQUE/AGGREGATE）
    时保留 tmp 表不删，由人工核对。
    """
    variant_enabled = bool(doris.get("variant_enabled", True))
    buckets = buckets or int(doris.get("default_buckets", 10))
    replication_num = int(doris.get("replication_num", 1))
    tmp = f"tmp_{table}"[:64]
    cols = ", ".join(f"`{m['doris_col']}`" for m in mapping)
    create_sql = build_create_table(db_name, table, mapping, variant_enabled,
                                    buckets, replication_num, ttl=ttl, model=model)
    insert_sql = (f"INSERT INTO `{db_name}`.`{table}` ({cols})\n"
                  f"SELECT {', '.join(select_exprs)} FROM `{db_name}`.`{tmp}`")
    partitions_added: list[str] = []
    c = _connect(doris)
    try:
        with c.cursor() as cur:
            # 上次迁移崩溃可能留下 tmp 表：RENAME 必失败且易误操作，先明确拒绝并给处理指引
            cur.execute(
                "SELECT TABLE_NAME FROM information_schema.tables "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (db_name, tmp),
            )
            if cur.fetchone():
                raise RuntimeError(
                    f"上次迁移遗留的临时表 `{db_name}`.`{tmp}` 仍存在，"
                    f"请人工核对后 DROP TABLE 删除再重试（表内是迁移前的旧数据）")
            cur.execute(f"ALTER TABLE `{db_name}`.`{table}` RENAME `{tmp}`")
            try:
                cur.execute(create_sql)
                if ttl:
                    # 动态分区表手动增删分区需先关开关（Doris 限制），补建历史分区后恢复
                    ttl_num, ttl_unit, ttl_col = _ttl_parts(ttl)
                    ms_epoch = any(m["doris_col"] == ttl_col and m.get("ms_epoch")
                                   for m in mapping)
                    cur.execute(f'ALTER TABLE `{db_name}`.`{table}` SET '
                                f'("dynamic_partition.enable" = "false")')
                    partitions_added = _provision_partitions(
                        cur, db_name, table, tmp, ttl_col, ttl_unit, ms_epoch)
                    cur.execute(f'ALTER TABLE `{db_name}`.`{table}` SET '
                                f'("dynamic_partition.enable" = "true")')
                cur.execute(insert_sql)
            except Exception:
                # 回滚：删半成品新表，tmp 改回原名（原表数据原样恢复）
                cur.execute(f"DROP TABLE IF EXISTS `{db_name}`.`{table}`")
                cur.execute(f"ALTER TABLE `{db_name}`.`{tmp}` RENAME `{table}`")
                raise
            cur.execute(f"SELECT COUNT(*) FROM `{db_name}`.`{tmp}`")
            old_cnt = int(cur.fetchone()[0])
            cur.execute(f"SELECT COUNT(*) FROM `{db_name}`.`{table}`")
            new_cnt = int(cur.fetchone()[0])
            tmp_dropped = False
            if new_cnt >= old_cnt or (model or "").upper() in ("UNIQUE", "AGGREGATE"):
                cur.execute(f"DROP TABLE `{db_name}`.`{tmp}`")
                tmp_dropped = True
    finally:
        c.close()
    return {"tmp": tmp, "old_rows": old_cnt, "new_rows": new_cnt,
            "tmp_dropped": tmp_dropped, "partitions_added": partitions_added,
            "ddl": create_sql + ";\n" + insert_sql + ";"}


def recreate_table(doris: dict, db_name: str, table: str, mapping: list[dict],
                   ttl: dict | None = None, model: str = "DUPLICATE",
                   buckets: int | None = None) -> dict:
    """删除并重建目标表（危险：表中数据全部丢失）。

    用途：给已存在的表启用 TTL 动态分区、变更表模型、清理历史遗留列
    （如旧版本的 etl_time/旧类型 kafka_ts，Doris 不支持删列/改列类型，只能重建）。
    """
    variant_enabled = bool(doris.get("variant_enabled", True))
    buckets = buckets or int(doris.get("default_buckets", 10))
    replication_num = int(doris.get("replication_num", 1))
    stmts = [
        f"CREATE DATABASE IF NOT EXISTS `{db_name}`",
        f"DROP TABLE IF EXISTS `{db_name}`.`{table}`",
        build_create_table(db_name, table, mapping, variant_enabled, buckets,
                           replication_num, ttl=ttl, model=model),
    ]
    c = _connect(doris)
    try:
        with c.cursor() as cur:
            for sql in stmts:
                cur.execute(sql)
    finally:
        c.close()
    return {"ddl": ";\n".join(stmts) + ";"}


# ---------------------------------------------------------------- 作业向导实时查询

_SYSTEM_DBS = ("information_schema", "mysql", "__internal_schema")
_DB_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def list_doris_dbs(env_dict: dict) -> list[str]:
    """实时查环境 Doris 的库列表（SHOW DATABASES，过滤系统库）；env_dict 为 envs.to_dict 形状。"""
    c = _connect(env_dict["doris"])
    try:
        with c.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return sorted(r[0] for r in cur.fetchall() if r[0] not in _SYSTEM_DBS)
    finally:
        c.close()


def list_doris_tables(env_dict: dict, db_name: str) -> list[str]:
    """实时查某库下的表列表；库名非法（防注入）直接返回空。"""
    if not _DB_NAME_RE.match(db_name or ""):
        return []
    c = _connect(env_dict["doris"])
    try:
        with c.cursor() as cur:
            cur.execute(f"SHOW TABLES FROM `{db_name}`")
            return sorted(r[0] for r in cur.fetchall())
    finally:
        c.close()
