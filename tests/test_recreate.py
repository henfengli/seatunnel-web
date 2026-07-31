# -*- coding: utf-8 -*-
"""表结构兼容性检查 / 数据迁移重建 / 提交与更新预检 测试（FakeConn 模拟 Doris，mock SeaTunnel）。"""
import json
import re
import threading
from datetime import datetime as _dt
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from app.models import Datasource, Environment, Job, JobEvent
from app.services import doris_ddl, orchestrator

from .helpers import check




# ---------------- 常量与 Fake ----------------
SHOW_CREATE_UNIQUE = """CREATE TABLE `t_u` (
  `id` BIGINT,
  `ts` DATETIMEV2(3),
  `v` VARCHAR(100)
) ENGINE=OLAP
UNIQUE KEY(`id`, `ts`)
PARTITION BY RANGE(`ts`)()
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"dynamic_partition.enable" = "true",
"dynamic_partition.time_unit" = "DAY",
"dynamic_partition.start" = "-7",
"dynamic_partition.end" = "3",
"dynamic_partition.prefix" = "p"
);"""


MAPPING = [
    {"source": "id", "st_type": "bigint", "doris_col": "id", "doris_type": "BIGINT",
     "nested": False, "is_key": True},
    {"source": "ts", "st_type": "timestamp", "doris_col": "ts", "doris_type": "DATETIMEV2(3)",
     "nested": False, "is_key": True},
    {"source": "v", "st_type": "string", "doris_col": "v", "doris_type": "STRING", "nested": False},
]
TTL = {"num": 7, "unit": "DAY", "column": "ts"}
OLD_COLS = {"id": "BIGINT", "ts": "DATETIMEV2(3)", "v": "STRING", "legacy": "STRING"}


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def execute(self, sql, args=None):
        self.conn.execs.append(sql)
        if "COLUMN_DEFAULT" in sql:
            self._rows = list(getattr(self.conn, "default_rows", []))
        elif "information_schema.columns" in sql:
            self._rows = list(self.conn.col_rows)
        elif sql.startswith("SHOW CREATE TABLE"):
            self._rows = [("t", self.conn.show_create)]
        elif sql.startswith("SELECT DISTINCT date_trunc"):
            self._rows = list(self.conn.trunc_rows)
        elif sql.startswith("SELECT COUNT(*)"):
            self._rows = [(self.conn.counts.pop(0),)]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, col_rows=(), show_create="", counts=(0, 0), trunc_rows=()):
        self.execs = []
        self.col_rows = col_rows
        self.show_create = show_create
        self.counts = list(counts)
        self.trunc_rows = list(trunc_rows)

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass


DORIS = {"fenodes": "fake:8030", "query_port": 9030, "username": "root", "password": "",
         "variant_enabled": True, "default_buckets": 4, "replication_num": 1}


# ---------------- 解析与变更级别 ----------------
def test_parse_show_create():
    # 1. SHOW CREATE 解析
    sc = doris_ddl._parse_show_create(SHOW_CREATE_UNIQUE)
    check("解析模型/key", sc["model"] == "UNIQUE" and sc["key_cols"] == ["id", "ts"], str(sc))
    check("解析分区/TTL/分桶", sc["partitioned"]
          and sc["ttl"] == {"unit": "DAY", "start": -7,
                            "create_history": False, "history_num": None}
          and sc["buckets"] == 4, str(sc))



def test_type_change_level():
    # 2. 类型变更分级
    check("同型 same", doris_ddl._type_change_level("BIGINT", "BIGINT") == "same")
    check("INT->BIGINT online", doris_ddl._type_change_level("INT", "BIGINT") == "online")
    check("VARCHAR 扩长 online", doris_ddl._type_change_level("VARCHAR(100)", "VARCHAR(512)") == "online")
    check("VARCHAR 缩短 recreate", doris_ddl._type_change_level("VARCHAR(512)", "VARCHAR(100)") == "recreate")
    check("STRING->VARCHAR recreate", doris_ddl._type_change_level("STRING", "VARCHAR(100)") == "recreate")
    check("DATE->DATETIME online", doris_ddl._type_change_level("DATE", "DATETIME") == "online")
    check("DATE->DATETIMEV2 online", doris_ddl._type_change_level("DATE", "DATETIMEV2(3)") == "online")
    check("BIGINT->INT recreate", doris_ddl._type_change_level("BIGINT", "INT") == "recreate")
    # 归一化：MySQL 协议显示名 vs Doris 规范名（同一类型不能误判）
    check("TINYINT(1)≡BOOLEAN", doris_ddl._type_change_level("TINYINT(1)", "BOOLEAN") == "same")
    check("BOOLEAN≡TINYINT(1)", doris_ddl._type_change_level("BOOLEAN", "TINYINT(1)") == "same")
    check("TEXT≡STRING", doris_ddl._type_change_level("TEXT", "STRING") == "same")
    check("string≡STRING", doris_ddl._type_change_level("string", "STRING") == "same")
    check("INT(11)≡INT", doris_ddl._type_change_level("INT(11)", "INT") == "same")
    check("tinyint(4)≡TINYINT", doris_ddl._type_change_level("tinyint(4)", "TINYINT") == "same")
    check("bigint(20)≡BIGINT", doris_ddl._type_change_level("bigint(20)", "BIGINT") == "same")
    check("decimalv3≡DECIMAL", doris_ddl._type_change_level("decimalv3(38,10)", "DECIMAL(38,10)") == "same")
    check("json≡JSONB", doris_ddl._type_change_level("json", "JSONB") == "same")
    # 官方清单：除 DATE/DATETIME 外都可转 STRING，STRING 不能转任何其他类型
    check("VARCHAR->STRING online", doris_ddl._type_change_level("VARCHAR(100)", "STRING") == "online")
    check("DATE->STRING recreate", doris_ddl._type_change_level("DATE", "STRING") == "recreate")
    check("DATETIME->STRING recreate", doris_ddl._type_change_level("DATETIME(3)", "STRING") == "recreate")

    # 2d. 全量归一化矩阵（逐条对照 BE schema_columns_scanner.cpp 的 COLUMN_TYPE 输出规则）
    SCAN_TO_CANON = {
        "tinyint(1)": "BOOLEAN", "tinyint(4)": "TINYINT", "smallint(6)": "SMALLINT",
        "int(11)": "INT", "bigint(20)": "BIGINT", "largeint": "LARGEINT",
        "float": "FLOAT", "double": "DOUBLE",
        "varchar(512)": "VARCHAR(512)", "char(20)": "CHAR(20)",
        "string": "STRING", "text": "STRING",
        "date": "DATEV2", "datetime": "DATETIMEV2(0)", "datetime(3)": "DATETIMEV2(3)",
        "decimal(38,10)": "DECIMAL(38,10)", "decimalv3(38,10)": "DECIMAL(38,10)",
        "json": "JSONB", "variant": "VARIANT",
        "hll": "HLL", "bitmap": "BITMAP", "ipv4": "IPV4", "ipv6": "IPV6",
    }
    for raw, expect in SCAN_TO_CANON.items():
        _b, _p = doris_ddl.canon_type(raw)
        got = f"{_b}({_p})" if _p else _b
        check(f"归一化 {raw}", got == expect, f"{got} != {expect}")
    check("DATETIME(3)≡DATETIMEV2(3)", doris_ddl._type_change_level("DATETIME(3)", "DATETIMEV2(3)") == "same")
    check("DATETIMEV2(3)≡DATETIME(3)", doris_ddl._type_change_level("DATETIMEV2(3)", "DATETIME(3)") == "same")
    check("DATETIME≡DATETIMEV2(0)", doris_ddl._type_change_level("DATETIME", "DATETIMEV2(0)") == "same")
    check("精度不同仍 recreate", doris_ddl._type_change_level("DATETIME", "DATETIMEV2(3)") == "recreate")



def test_compat_decision():
    sc = doris_ddl._parse_show_create(SHOW_CREATE_UNIQUE)
    # 3. compat_decision 分级
    r = doris_ddl.compat_decision(None, None, MAPPING, TTL, "UNIQUE", 4)
    check("表不存在 level=none", r["level"] == "none")
    r = doris_ddl.compat_decision(OLD_COLS, sc, MAPPING, TTL, "UNIQUE", 4)
    check("一致 level=same", r["level"] == "same", str(r["reasons"]) + str(r["online"]))
    check("遗留列记 dropped", r["dropped"] == ["legacy"], str(r["dropped"]))
    r = doris_ddl.compat_decision({"id": "BIGINT", "ts": "DATETIMEV2(3)", "v": "VARCHAR(100)"},
                                  sc, MAPPING, TTL, "UNIQUE", 4)
    check("VARCHAR->STRING online", r["level"] == "online"
          and r["type_changes"] == [("v", "VARCHAR(100)", "STRING")], str(r))
    r = doris_ddl.compat_decision(OLD_COLS, sc, MAPPING, TTL, "DUPLICATE", 4)
    check("模型不一致 recreate", r["level"] == "recreate" and any("表模型" in x for x in r["reasons"]))
    r = doris_ddl.compat_decision(OLD_COLS, sc, MAPPING, None, "UNIQUE", 4)
    check("去掉 TTL 无 online 动作", r["level"] == "same" or all("动态分区" not in a for a in r["online"]))
    r = doris_ddl.compat_decision({"id": "INT", "ts": "DATETIMEV2(3)", "v": "STRING"}, sc,
                                  MAPPING, TTL, "UNIQUE", 4)
    check("类型放大 online", r["level"] == "online" and r["type_changes"] == [("id", "INT", "BIGINT")],
          str(r))
    sc_no_ttl = doris_ddl._parse_show_create(SHOW_CREATE_UNIQUE.replace('"true"', '"false"'))
    r = doris_ddl.compat_decision(OLD_COLS, sc_no_ttl, MAPPING, TTL, "UNIQUE", 4)
    check("已分区未启用动态分区 -> online ALTER", r["level"] == "online" and r["ttl_alter"], str(r))
    r = doris_ddl.compat_decision(OLD_COLS, sc, MAPPING, {**TTL, "history_num": 120}, "UNIQUE", 4)
    check("预建历史分区缺失 -> online ALTER", r["level"] == "online"
          and r["ttl_alter"].get("history_num") == 120, str(r["ttl_alter"]))

    sc_nopart = doris_ddl._parse_show_create(
        "CREATE TABLE `t` (\n `id` BIGINT\n)\nDUPLICATE KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 3")
    r = doris_ddl.compat_decision({"id": "BIGINT"}, sc_nopart, MAPPING[:1], TTL, "DUPLICATE", 3)
    check("未分区启用 TTL -> recreate", r["level"] == "recreate"
          and any("未分区" in x for x in r["reasons"]), str(r["reasons"]))
    # kafka_ipp 实际案例：老表 TINYINT(1) vs 平台建的 BOOLEAN——同一样东西，不能误判冲突
    r = doris_ddl.compat_decision(
        {"synthetic_flag": "TINYINT(1)"}, sc_nopart,
        [{"source": "synthetic_flag", "st_type": "boolean", "doris_col": "synthetic_flag",
          "doris_type": "BOOLEAN", "nested": False}],
        None, "DUPLICATE", 3)
    check("TINYINT(1) 表 vs BOOLEAN 映射不误判", r["level"] == "same", str(r))
    # kafka_ipp 实际案例 2：平台建的 DATETIMEV2(3)，information_schema 读回 datetime(3)，不能误判
    ts_map = [
        {"source": "gen_time", "st_type": "bigint", "doris_col": "gen_time",
         "doris_type": "DATETIMEV2(3)", "nested": False, "ms_epoch": True, "is_key": True},
        {"source": "kafka_ts", "st_type": "bigint", "doris_col": "kafka_ts",
         "doris_type": "DATETIMEV2(3)", "nested": False, "sink_only": True},
    ]
    r = doris_ddl.compat_decision(
        {"gen_time": "DATETIME(3)", "kafka_ts": "DATETIME(3)"}, sc_nopart,
        ts_map, None, "DUPLICATE", 3)
    check("DATETIME(3) 表 vs DATETIMEV2(3) 映射不误判", r["level"] == "same", str(r))
    # OrderEvent 实际案例：平台建的 STRING，information_schema 读回 text，不能误判也不能发无效 MODIFY
    r = doris_ddl.compat_decision(
        {"id": "BIGINT", "name": "TEXT"}, sc_nopart,
        [{"source": "id", "st_type": "bigint", "doris_col": "id", "doris_type": "BIGINT",
          "nested": False, "is_key": True},
         {"source": "name", "st_type": "string", "doris_col": "name",
          "doris_type": "STRING", "nested": False}],
        None, "DUPLICATE", 3)
    check("TEXT 表 vs STRING 映射不误判、无 online 动作",
          r["level"] == "same" and not r["online"], str(r))



def test_partition_span():
    # 2b. 分区跨度
    check("分区跨度 DAY", doris_ddl._partition_span(_dt(2026, 7, 18), "DAY")
          == ("p20260718", "2026-07-18", "2026-07-19"))
    check("分区跨度 MONTH 跨年", doris_ddl._partition_span(_dt(2026, 12, 1), "MONTH")
          == ("p202612", "2026-12-01", "2027-01-01"))



# ---------------- 迁移计划与执行 ----------------
def test_migration_plan():
    # 4. 迁移计划与 SELECT 表达式
    MAPPING_V_INT = {"source": "v", "st_type": "string", "doris_col": "v",
                     "doris_type": "INT", "nested": False}
    plan, dropped = doris_ddl.build_migration_plan(
        {"id": "INT", "v": "STRING", "junk": "BIGINT"},
        [MAPPING[0], MAPPING_V_INT,
         {"source": "newc", "st_type": "bigint", "doris_col": "newc", "doris_type": "BIGINT",
          "nested": False}],
        True, ["id"])
    kinds = {p["col"]: p["kind"] for p in plan}
    check("迁移计划分级", kinds == {"id": "cast_safe", "v": "cast_risky", "newc": "missing"},
          str(kinds))
    check("dropped 识别", dropped == ["junk"], str(dropped))
    exprs, errs = doris_ddl.build_select_exprs(plan, {})
    check("缺省决策表达式", not errs and "CAST(`id` AS BIGINT) AS `id`" in exprs
          and "CAST(`v` AS INT) AS `v`" in exprs and "NULL AS `newc`" in exprs, str(exprs) + str(errs))
    exprs, errs = doris_ddl.build_select_exprs(plan, {"conv_v": "const", "fillv_v": "'N/A'",
                                                      "fill_newc": "0"})
    check("用户决策：常量填充", not errs and "'N/A' AS `v`" in exprs and "0 AS `newc`" in exprs,
          str(exprs))
    exprs, errs = doris_ddl.build_select_exprs(plan, {"fill_newc": "1; DROP TABLE t"})
    check("非法填充值拦截", errs and "newc" in errs[0], str(errs))

    # 4b. ms_epoch 列迁移：BIGINT epoch 毫秒 -> DATETIMEV2(3) 必须 from_millisecond，不能 CAST
    plan_ms, _ = doris_ddl.build_migration_plan(
        {"gen_time": "BIGINT"},
        [{"source": "gen_time", "st_type": "bigint", "doris_col": "gen_time",
          "doris_type": "DATETIMEV2(3)", "nested": False, "ms_epoch": True}],
        True, ["gen_time"])
    check("ms_epoch 计划", plan_ms[0]["kind"] == "ms_epoch", str(plan_ms))
    exprs, errs = doris_ddl.build_select_exprs(plan_ms, {})
    check("ms_epoch 表达式（量级自适应）", len(exprs) == 1
          and exprs[0].startswith("from_millisecond(CASE WHEN `gen_time` >= 100000000000000000")
          and exprs[0].endswith("END) AS `gen_time`"), str(exprs))




def test_migrate_table(monkeypatch):
    # 5. migrate_table 全流程（FakeConn）
    fc = FakeConn(counts=(100, 100))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc)
    mig = doris_ddl.migrate_table(DORIS, "db1", "t", MAPPING, TTL, "UNIQUE", 4,
                                  ["`id`", "`ts`", "`v`"])
    check("迁移 RENAME->CREATE->INSERT", "ALTER TABLE `db1`.`t` RENAME `tmp_t`" in fc.execs
          and any(s.startswith("CREATE TABLE IF NOT EXISTS") for s in fc.execs)
          and any(s.startswith("INSERT INTO `db1`.`t`") for s in fc.execs), str(fc.execs[:4]))
    check("行数一致删 tmp", mig["tmp_dropped"] and fc.execs[-1] == "DROP TABLE `db1`.`tmp_t`")

    # TTL 迁移：关动态分区开关 -> 按数据补建历史分区 -> 开开关 -> INSERT
    fc_ttl = FakeConn(counts=(100, 100),
                      trunc_rows=[(_dt(2026, 7, 18),), (_dt(2026, 7, 27),)])
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc_ttl)
    mig_ttl = doris_ddl.migrate_table(DORIS, "db1", "t", MAPPING, TTL, "UNIQUE", 4,
                                      ["`id`", "`ts`", "`v`"])
    ex = fc_ttl.execs
    check("补建历史分区语句", "ALTER TABLE `db1`.`t` ADD PARTITION IF NOT EXISTS `p20260718` "
          "VALUES [('2026-07-18'), ('2026-07-19'))" in ex, str(ex))
    i_false = next(i for i, s in enumerate(ex) if s.startswith("ALTER TABLE")
                   and 'dynamic_partition.enable" = "false"' in s)
    i_add = next(i for i, s in enumerate(ex) if "ADD PARTITION" in s)
    i_true = next(i for i, s in enumerate(ex) if s.startswith("ALTER TABLE")
                  and 'dynamic_partition.enable" = "true"' in s)
    i_insert = next(i for i, s in enumerate(ex) if s.startswith("INSERT INTO"))
    check("补分区顺序：关->补建->开->INSERT", i_false < i_add < i_true < i_insert)
    check("partitions_added 记录", mig_ttl["partitions_added"] == ["p20260718", "p20260727"],
          str(mig_ttl["partitions_added"]))

    fc2 = FakeConn(counts=(100, 90))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc2)
    mig2 = doris_ddl.migrate_table(DORIS, "db1", "t", MAPPING, TTL, "DUPLICATE", 4,
                                   ["`id`", "`ts`", "`v`"])
    check("行数不一致保留 tmp", not mig2["tmp_dropped"]
          and not any(s == "DROP TABLE `db1`.`tmp_t`" for s in fc2.execs))

    class FailInsertConn(FakeConn):
        def cursor(self):
            cur = FakeCursor(self)
            orig_exec = cur.execute

            def _exec(sql, args=None):
                if sql.startswith("INSERT INTO"):
                    raise RuntimeError("mock insert failed")
                return orig_exec(sql, args)
            cur.execute = _exec
            return cur

    fc3 = FailInsertConn(counts=(0, 0))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc3)
    try:
        doris_ddl.migrate_table(DORIS, "db1", "t", MAPPING, TTL, "DUPLICATE", 4, ["`id`"])
        check("迁移失败应抛异常", False)
    except RuntimeError:
        check("迁移失败抛异常", True)
    check("失败回滚表名", "DROP TABLE IF EXISTS `db1`.`t`" in fc3.execs
          and "ALTER TABLE `db1`.`tmp_t` RENAME `t`" in fc3.execs, str(fc3.execs))

    # tmp 表残留守卫：上次迁移崩溃留下 tmp_t，直接拒绝并给处理指引
    class TmpLeftoverConn(FakeConn):
        def cursor(self):
            cur = FakeCursor(self)
            orig_exec = cur.execute

            def _exec(sql, args=None):
                if "information_schema.tables" in sql:
                    cur._rows = [("tmp_t",)]
                    return
                return orig_exec(sql, args)
            cur.execute = _exec
            return cur

    fc_tmp = TmpLeftoverConn(counts=(0, 0))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc_tmp)
    try:
        doris_ddl.migrate_table(DORIS, "db1", "t", MAPPING, TTL, "DUPLICATE", 4, ["`id`"])
        check("tmp 残留应拒绝", False)
    except RuntimeError as e:
        check("tmp 残留拒绝并指引", "遗留的临时表" in str(e), str(e)[:80])
    check("tmp 残留未执行 RENAME", not any("RENAME" in s for s in fc_tmp.execs))



def test_ensure_table(monkeypatch):
    # 6. ensure_table：recreate 拒绝 / online 演进
    fc4 = FakeConn(col_rows=[("id", "bigint"), ("ts", "datetimev2(3)"), ("v", "varchar(100)")],
                   show_create=SHOW_CREATE_UNIQUE)
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc4)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", MAPPING, TTL, "DUPLICATE", 4)
    check("ensure recreate 拒绝", r.get("needs_recreate") and any("表模型" in x for x in r["reasons"]))
    ddl_execs = [s for s in fc4.execs
                 if s.startswith(("ALTER TABLE", "CREATE TABLE", "DROP TABLE", "INSERT INTO"))]
    check("recreate 未执行 DDL", ddl_execs == [], str(fc4.execs))

    fc5 = FakeConn(col_rows=[("id", "int"), ("ts", "datetimev2(3)"), ("v", "varchar(100)")],
                   show_create=SHOW_CREATE_UNIQUE.replace('"true"', '"false"'))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc5)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", MAPPING, TTL, "UNIQUE", 4)
    alters = [s for s in fc5.execs if s.startswith("ALTER TABLE")]
    check("online MODIFY COLUMN", any("MODIFY COLUMN `id` BIGINT KEY" in s for s in alters),
          str(alters))
    check("online TTL ALTER", r.get("ttl_altered")
          and any("dynamic_partition.start" in s for s in alters), str(alters))

    # MODIFY COLUMN 必须重述原 DEFAULT（Doris 文档：MODIFY 需声明完整列信息，否则默认值被清掉）
    fc5d = FakeConn(col_rows=[("id", "int"), ("ts", "datetimev2(3)"), ("v", "varchar(100)")],
                    show_create=SHOW_CREATE_UNIQUE)
    fc5d.default_rows = [("v", "'abc'")]
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc5d)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", MAPPING, TTL, "UNIQUE", 4)
    check("MODIFY 重述 DEFAULT",
          any("MODIFY COLUMN `v` STRING DEFAULT 'abc'" in s for s in fc5d.execs),
          str([s for s in fc5d.execs if "MODIFY" in s]))

    # ALTER 时同步建历史分区：关开关 -> 按当前时间往前建 N 个 -> 恢复配置（不等调度器）
    fc_hn = FakeConn(col_rows=[("id", "int"), ("ts", "datetimev2(3)"), ("v", "varchar(100)")],
                     show_create=SHOW_CREATE_UNIQUE.replace('"true"', '"false"'))
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc_hn)
    TTL_HN = {"num": 7, "unit": "HOUR", "column": "ts", "history_num": 3}
    r = doris_ddl.ensure_table(DORIS, "db1", "t", MAPPING, TTL_HN, "UNIQUE", 4)
    execs = fc_hn.execs
    i_off = next((i for i, s in enumerate(execs) if 'dynamic_partition.enable" = "false"' in s), -1)
    adds = [s for s in execs if "ADD PARTITION IF NOT EXISTS" in s]
    i_set = next((i for i, s in enumerate(execs)
                  if s.startswith("ALTER TABLE `db1`.`t` SET") and "create_history_partition" in s), -1)
    check("同步建历史分区顺序与数量", 0 <= i_off < min(i for i, s in enumerate(execs)
          if "ADD PARTITION" in s) and len(adds) == 3 and i_set > i_off, str(execs))
    check("恢复配置带 history_partition_num", '"dynamic_partition.history_partition_num" = "3"' in execs[i_set])
    check("分区名按小时格式", all(re.search(r"`p\d{10}` VALUES", s) for s in adds), str(adds[0]))


    # AGGREGATE 模型 MODIFY：非分桶 key 列带 KEY，value 列带聚合函数
    # （分桶列=首 key 列禁止修改，本用例 id 类型一致不触发；改的是非首 key 列 k2 和 value 列 cnt）
    AGG_MAP = [
        {"source": "id", "st_type": "bigint", "doris_col": "id", "doris_type": "BIGINT",
         "nested": False, "is_key": True},
        {"source": "k2", "st_type": "bigint", "doris_col": "k2", "doris_type": "BIGINT",
         "nested": False, "is_key": True},
        {"source": "cnt", "st_type": "bigint", "doris_col": "cnt", "doris_type": "BIGINT",
         "nested": False, "agg": "SUM"},
    ]
    fc_agg = FakeConn(
        col_rows=[("id", "bigint"), ("k2", "int"), ("cnt", "int")],
        show_create="CREATE TABLE `t` (\n `id` BIGINT,\n `k2` INT,\n `cnt` INT SUM\n)\n"
                    "AGGREGATE KEY(`id`, `k2`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 3")
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc_agg)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", AGG_MAP, None, "AGGREGATE", 3)
    alters = [s for s in fc_agg.execs if "MODIFY COLUMN" in s]
    check("AGG 非分桶 key 列 MODIFY 带 KEY", any("MODIFY COLUMN `k2` BIGINT KEY" in s for s in alters),
          str(alters))
    check("AGG value 列 MODIFY 带 SUM", any("MODIFY COLUMN `cnt` BIGINT SUM" in s for s in alters),
          str(alters))

    # 分桶列（首 key 列）类型变化 -> 直接判 recreate（Doris 禁止修改分桶列）
    fc_bucket = FakeConn(
        col_rows=[("id", "int"), ("k2", "bigint"), ("cnt", "int")],
        show_create="CREATE TABLE `t` (\n `id` INT,\n `k2` BIGINT,\n `cnt` INT SUM\n)\n"
                    "AGGREGATE KEY(`id`, `k2`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 3")
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc_bucket)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", AGG_MAP, None, "AGGREGATE", 3)
    check("分桶列类型变化判 recreate", r.get("needs_recreate")
          and any("分桶列" in x for x in r["reasons"]), str(r.get("reasons")))

    fc6 = FakeConn(col_rows=[("id", "int"), ("ts", "datetimev2(3)"), ("v", "varchar(100)")],
                   show_create=SHOW_CREATE_UNIQUE)
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc6)
    r = doris_ddl.ensure_table(DORIS, "db1", "t", MAPPING, TTL, "UNIQUE", 4, dry_run=True)
    check("dry_run 不执行", not r.get("needs_recreate")
          and not any(s.startswith("ALTER TABLE") for s in fc6.execs), str(fc6.execs))



def test_busy_retry(monkeypatch):
    # 7b. 表忙（SCHEMA_CHANGE 状态）自动轮询重试；超时带指引抛出
    class BusyCursor(FakeCursor):
        def execute(self, sql, args=None):
            if sql.startswith("ALTER TABLE") and self.conn.busy_count > 0:
                self.conn.execs.append(sql)
                self.conn.busy_count -= 1
                raise RuntimeError("(1105, 'errCode = 2, detailMessage = Table[t]'s "
                                   "state(SCHEMA_CHANGE) is not NORMAL. "
                                   "Do not allow doing ALTER ops')")
            return super().execute(sql, args)

    class BusyConn(FakeConn):
        def __init__(self, busy):
            super().__init__()
            self.busy_count = busy

        def cursor(self):
            return BusyCursor(self)

    monkeypatch.setattr(doris_ddl.time, "sleep", lambda s: None)
    fc8 = BusyConn(2)
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc8)
    doris_ddl._exec_all(fc8, ['ALTER TABLE `db1`.`t` SET ("a" = "b")'], tolerate_noop=True)
    check("SCHEMA_CHANGE 忙重试后成功", len(fc8.execs) == 3, str(len(fc8.execs)))
    fc9 = BusyConn(999)
    monkeypatch.setattr(doris_ddl, "connect", lambda d: fc9)
    try:
        doris_ddl._exec_all(fc9, ['ALTER TABLE `db1`.`t` SET ("a" = "b")'], max_wait_sec=4)
        check("忙超时应抛异常", False)
    except RuntimeError as e:
        check("忙超时带指引", "SHOW ALTER TABLE COLUMN" in str(e), str(e)[:80])




# ---------------------------------------------------------------- 迁移重建编排端到端（orchestrator.migrate_recreate）
def _mk_migrate_job(db, name="mg_e2e") -> Job:
    """造一个 DRAFT 作业（env=demo，Doris 连接由 monkeypatch 替换为 FakeConn）。"""
    env = db.query(Environment).filter_by(name="demo").first()
    if env is None:
        env = Environment(name="demo", doris_fenodes="fake:8030", doris_query_port=9030,
                          doris_username="root", doris_password="", variant_enabled=True,
                          seatunnel_masters="http://127.0.0.1:18082")
        db.add(env)
    ds = db.query(Datasource).filter_by(env="demo", name="mg").first()
    if ds is None:
        ds = Datasource(env="demo", name="mg", type="kafka", connection={"servers": "k:9092"})
        db.add(ds)
    db.commit()
    job = Job(name=name, env="demo", biz_line="db1", source_type="kafka",
              datasource_id=ds.id, source_ref="ticks", doris_db="db1", doris_table="t",
              field_mapping=MAPPING, options={}, status="DRAFT")
    db.add(job)
    db.commit()
    return job


def test_migrate_recreate_e2e(db, monkeypatch):
    """编排端到端：compat -> 迁移 -> 事件留档（DRAFT 作业不动 SeaTunnel）。"""
    job = _mk_migrate_job(db)
    fc = FakeConn(col_rows=[("id", "bigint"), ("ts", "datetimev2(3)"), ("v", "varchar(512)")],
                  show_create=SHOW_CREATE_UNIQUE, counts=(100, 100))
    monkeypatch.setattr(doris_ddl, "connect", lambda d, **kw: fc)
    res = orchestrator.migrate_recreate(db, job, {})
    check("迁移编排成功", res["ok"], str(res))
    check("消息带行数", "100 -> 100 行" in res["msg"], res.get("msg", ""))
    events = [e.detail for e in db.query(JobEvent).filter_by(job_id=job.id, event="migrate")]
    check("迁移事件留档", any("数据迁移完成" in d for d in events), str(events))
    check("DRAFT 作业状态不动", job.status == "DRAFT", job.status)


def test_migrate_recreate_rollback(db, monkeypatch):
    """迁移中途失败：表回滚 + 事件留档 + ok=False（DRAFT 作业无恢复动作）。"""
    job = _mk_migrate_job(db, name="mg_e2e_fail")

    class FailCursor(FakeCursor):
        def execute(self, sql, args=None):
            if sql.startswith("INSERT INTO"):  # 建表后灌数时失败，触发回滚
                self.conn.execs.append(sql)
                raise RuntimeError("insert burst")
            return super().execute(sql, args)

    class FailConn(FakeConn):
        def cursor(self):
            return FailCursor(self)

    fc = FailConn(col_rows=[("id", "bigint"), ("ts", "datetimev2(3)"), ("v", "varchar(512)")],
                  show_create=SHOW_CREATE_UNIQUE, counts=(100, 100))
    monkeypatch.setattr(doris_ddl, "connect", lambda d, **kw: fc)
    res = orchestrator.migrate_recreate(db, job, {})
    check("失败返回 ok=False", not res["ok"], str(res))
    check("错误说明已回滚", "已回滚为原表" in res["error"], res.get("error", ""))
    events = [e.detail for e in db.query(JobEvent).filter_by(job_id=job.id, event="migrate")]
    check("失败事件留档", any("已回滚为原表" in d for d in events), str(events))


# ---------------------------------------------------------------- 提交/更新预检（mock SeaTunnel）
ST = {"status": "RUNNING", "stops": 0, "submits": []}


class MockSeaTunnel(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace")
        if u.path == "/submit-job":
            ST["status"] = "RUNNING"
            ST["submits"].append(dict(q))
            self._json({"jobId": "733584788375666689", "jobName": "mock"})
        elif u.path == "/stop-job":
            if not body:
                self._json({"error": "Request body is empty."}, 400)
                return
            payload = json.loads(body)
            ST["stops"] += 1
            ST["status"] = "FINISHED" if payload.get("isStopWithSavePoint") else "CANCELED"
            self._json({"jobId": str(payload.get("jobId", "")), "jobName": "mock"})
        else:
            self._json({"error": "not found"}, 404)

    def do_GET(self):
        if self.path.startswith("/job-info"):
            self._json({"jobId": "733584788375666689", "jobName": "mock",
                        "jobStatus": ST["status"], "createTime": 1, "metrics": {}})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, *a):
        pass


@pytest.fixture(scope="module", autouse=True)
def mock_st():
    server = HTTPServer(("127.0.0.1", 18082), MockSeaTunnel)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield
    server.shutdown()


def test_submit_precheck(db):
    """提交/更新预检拒绝（mock SeaTunnel，验证作业状态不被破坏）。"""
    mapping = [
        {"source": "id", "st_type": "bigint", "doris_col": "id", "doris_type": "BIGINT",
         "nested": False, "is_key": True},
        {"source": "ts", "st_type": "timestamp", "doris_col": "ts", "doris_type": "DATETIMEV2(3)",
         "nested": False, "is_key": True},
        {"source": "v", "st_type": "string", "doris_col": "v", "doris_type": "STRING",
         "nested": False},
    ]
    env = db.query(Environment).filter_by(name="dev").first()
    if env is None:
        env = Environment(name="dev", doris_fenodes="127.0.0.1:8030", doris_query_port=9030,
                          doris_username="root", doris_password="", variant_enabled=True)
    env.seatunnel_masters = "http://127.0.0.1:18082"
    db.add(env)
    ds = db.query(Datasource).filter_by(env="dev", name="rc").first()
    if ds is None:
        ds = Datasource(env="dev", name="rc", type="kafka", connection={"servers": "k:9092"})
        db.add(ds)
    db.commit()

    job = Job(name="rc_test", env="dev", biz_line="db1", source_type="kafka", datasource_id=ds.id,
              source_ref="ticks", doris_db="db1", doris_table="t",
              field_mapping=mapping, options={},
              status="DRAFT", seatunnel_job_id="733584788375666689")
    db.add(job)
    db.commit()

    orig_ensure = doris_ddl.ensure_table
    try:
        # 提交：needs_recreate -> 拒绝，状态不变，未提交 SeaTunnel
        doris_ddl.ensure_table = lambda *a, **kw: {
            "created": False, "added_columns": [], "ddl": "", "compat": {},
            "needs_recreate": True, "reasons": ["表模型不一致：现有 DUPLICATE，当前配置 UNIQUE"]}
        r = orchestrator.submit(db, job)
        check("提交被预检拒绝", r.get("ok") is False and r.get("needs_recreate"), str(r))
        check("拒绝后仍 DRAFT 且未提交", job.status == "DRAFT" and not ST["submits"], job.status)

        # 更新：预检拒绝，RUNNING 作业保持运行（不停）
        job.status = "RUNNING"
        db.add(job)
        db.commit()
        r = orchestrator.update_and_restart(db, job, note="t")
        check("更新被预检拒绝", r.get("ok") is False and r.get("stage") == "precheck", str(r))
        check("作业保持 RUNNING 未停", job.status == "RUNNING" and ST["stops"] == 0, job.status)

        # STOPPED 更新并重启：SeaTunnel 侧旧作业还在跑（mock 返回 RUNNING）时先停再等终态，再带 savepoint 提交
        doris_ddl.ensure_table = lambda *a, **kw: {"created": False, "added_columns": [],
                                                   "ddl": "-- ok", "compat": {}}
        job.status = "STOPPED"
        db.add(job)
        db.commit()
        r = orchestrator.update_and_restart(db, job, note="t2")
        check("STOPPED 更新并重启成功", r.get("ok") is True, str(r))
        check("旧作业在跑先停再等终态", ST["stops"] == 1, str(ST["stops"]))
        check("带 savepoint 提交", ST["submits"] and ST["submits"][-1].get("isStartWithSavePoint") == ["true"],
              str(ST["submits"]))
        check("状态 RUNNING", job.status == "RUNNING", job.status)
    finally:
        doris_ddl.ensure_table = orig_ensure
