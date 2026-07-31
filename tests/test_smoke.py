# -*- coding: utf-8 -*-
"""冒烟测试：proto 解析 + 字段映射 + 渲染管线（不依赖外部服务）。按业务段拆分，失败看用例名定位。"""
import pytest
from bson import Int64
from sqlalchemy import text
from starlette.datastructures import FormData

from app.api.pages.common import collect_options, parse_mapping_form
from app.api.pages.datasource import build_connection
from app.core.crypto import encrypt, sanitize_error
from app.models import Datasource, Environment, Job, ProtoPackage
from app.services import doris_ddl, field_mapping, proto_center, render
from app.services.metadata import mongo_d
from app.templating import _mask_conf

from .helpers import check

FLAT_PROTO = '''
syntax = "proto3";
package pricing;
message InstrumentPricingParameters {
  int64 gen_time = 1;
  string instrument_id = 6;
  double theo = 8;
  bool synthetic_flag = 130;
}
'''

NESTED_PROTO = '''
syntax = "proto3";
message Event {
    fixed64 produce_time = 1;
    DepthMarketData depth_market_data = 16;
}
message MDEntry {
    double price = 1;
    int32 volume = 2;
    int32 orders = 3;
}
message DepthMarketData {
    int32 instrument_index = 1;
    repeated MDEntry bid_order_book = 4;
}
'''


# ---------------------------------------------------------------- 共享种子（模块级）


@pytest.fixture(scope="module")
def dev_env(db):
    """渲染管线 envs.get_env 依赖的 dev 环境。"""
    env = db.query(Environment).filter_by(name="dev").first()
    if env is None:
        env = Environment(name="dev", seatunnel_masters="http://127.0.0.1:8080",
                          doris_fenodes="127.0.0.1:8030", doris_query_port=9030,
                          doris_username="root", doris_password="",
                          variant_enabled=True, default_buckets=10)
        db.add(env)
        db.commit()
    return env


@pytest.fixture(scope="module")
def nested_pkg(db, dev_env):
    pkg = ProtoPackage(name="t_nested", content=NESTED_PROTO,
                       parsed=proto_center.parse_proto(NESTED_PROTO))
    db.add(pkg)
    db.commit()
    return pkg


@pytest.fixture(scope="module")
def event_fields(nested_pkg):
    return proto_center.schema_fields_for(nested_pkg, "Event")


@pytest.fixture(scope="module")
def ts_mapping(event_fields):
    return field_mapping.append_timestamp_columns(
        field_mapping.build_mapping("kafka", event_fields, True), "kafka")


def _mk_kafka_job(db, pkg, name, mapping):
    """最小 kafka 作业（渲染用）：独立数据源，避免测试间互相污染。"""
    ds = Datasource(env="dev", name=f"ds_{name}", type="kafka",
                    connection={"servers": "kafka1:9092"})
    db.add(ds)
    db.commit()
    job = Job(name=name, env="dev", biz_line="md", source_type="kafka",
              datasource_id=ds.id, source_ref="to_doris",
              doris_db="seatunnel_sync", doris_table=f"t_{name}",
              proto_package_id=pkg.id, message_name="Event",
              field_mapping=mapping, options={})
    job.datasource = ds
    job.proto_package = pkg
    db.add(job)
    db.commit()
    return job



def test_proto_parse():
    """扁平/嵌套 proto 解析 + subset_proto 依赖闭包裁剪"""
    # 1. 扁平 proto 解析
    p1 = proto_center.parse_proto(FLAT_PROTO)
    check("flat top_level", p1["top_level"] == ["InstrumentPricingParameters"], str(p1["top_level"]))

    # 2. 嵌套 proto 解析
    p2 = proto_center.parse_proto(NESTED_PROTO)
    check("nested top_level", set(p2["top_level"]) == {"Event", "MDEntry", "DepthMarketData"}, str(p2["top_level"]))

    # 2c. subset_proto：按选中 message 裁剪依赖闭包（Event -> DepthMarketData -> MDEntry 全保留）
    MULTI_PROTO = NESTED_PROTO + '''
    message Unrelated {
        string foo = 1;
    }
    '''
    sub = proto_center.subset_proto(MULTI_PROTO, "Event")
    check("subset 保留 header", 'syntax = "proto3"' in sub)
    check("subset 保留依赖闭包", "message Event" in sub and "message DepthMarketData" in sub
          and "message MDEntry" in sub)
    check("subset 剔除无关 message", "message Unrelated" not in sub)
    check("subset 找不到目标回退原文", proto_center.subset_proto(MULTI_PROTO, "Nope") == MULTI_PROTO)
    # 用户真实场景：多顶层 message 只取一个；注释归属于其后的 message（被剔除的不混入 header）
    USER_PROTO = '''
    syntax = "proto3";
    package derivatives.view.pb;

    // TheoPrice
    message TheoPrice {
        int64 gen_time = 1;
    }

    // InstrumentPricingParameters
    // unique_key: seq_num + pricing_profile + instrument
    message InstrumentPricingParameters {
        int64 gen_time = 1;
        TheoPrice theo_price = 2;
    }

    // VolDynamics
    message VolDynamics {
        string x = 1;
    }
    '''
    sub2 = proto_center.subset_proto(USER_PROTO, "InstrumentPricingParameters")
    check("用户场景：选中+被引用保留", "message InstrumentPricingParameters" in sub2
          and "message TheoPrice" in sub2 and "package derivatives.view.pb;" in sub2)
    check("用户场景：无关剔除", "message VolDynamics" not in sub2)
    check("选中 message 的注释保留", "// InstrumentPricingParameters" in sub2
          and "// unique_key" in sub2 and "// TheoPrice" in sub2)
    check("被剔除 message 的注释不混入", "// VolDynamics" not in sub2)

def test_mongo_bson_types():
    """Mongo BSON 类型精确判定（不按值大小猜）+ mongo 映射"""
    # 2d. Mongo BSON 类型精确判定（不按值大小猜）：int64 用 bson.Int64 区分，
    # int32->INT、int64->BIGINT（OrderEvent exchange_time 案例：值为 0 也不能误判）

    check("bson Int64 -> int64", mongo_d._bson_type(Int64(0)) == "int64")
    check("原生 int -> int32", mongo_d._bson_type(0) == "int32")
    check("大值原生 int 仍 int32", mongo_d._bson_type(99999999999) == "int32")
    mm = field_mapping.build_mapping("mongodb", [{"name": "exchange_time", "type": "int64"},
                                                 {"name": "qty", "type": "int32"}], True)
    check("mongo int64 -> BIGINT", mm[0]["st_type"] == "bigint" and mm[0]["doris_type"] == "BIGINT")
    check("mongo int32 -> INT", mm[1]["st_type"] == "int" and mm[1]["doris_type"] == "INT")
    # mongo array -> array<string>（若声明 string，连接器会把数组包成 {"_value":[...]}，已验证两种 serde 一致）
    arr = field_mapping.build_mapping("mongodb", [{"name": "tags", "type": "array"}], True)[0]
    check("mongo array -> array<string> + VARIANT", arr["st_type"] == "array<string>"
          and arr["doris_type"] == "VARIANT" and arr["nested"] is True, str(arr))

def test_enum_field():
    """proto enum -> string"""
    # 2e. proto enum -> string（SeaTunnel 对 INT 原样返回 EnumValueDescriptor 会炸；
    # STRING 分支 toString() 输出枚举名）
    ENUM_PROTO = '''
    syntax = "proto3";
    enum Status { UNKNOWN = 0; ACTIVE = 1; }
    message Order { Status status = 1; string symbol = 2; }
    '''
    pkg_e = ProtoPackage(name="t_enum", content=ENUM_PROTO,
                         parsed=proto_center.parse_proto(ENUM_PROTO))
    ef = {f["name"]: f["st_type"] for f in proto_center.schema_fields_for(pkg_e, "Order")}
    check("enum 字段 -> string", ef.get("status") == "string", str(ef))

def test_doris_type_canon():
    """doris 源 COLUMN_TYPE 归一化（decimalv3 不丢精度、tinyint(1) 还原 BOOLEAN）"""
    # 2f. doris 源 COLUMN_TYPE 归一化（decimalv3 不丢精度、tinyint(1) 还原 BOOLEAN）
    dm = field_mapping.build_mapping("doris", [{"name": "amt", "type": "decimalv3(38,10)"},
                                               {"name": "flag", "type": "tinyint(1)"},
                                               {"name": "ts", "type": "datetime(3)"}], True)
    check("doris decimalv3 -> DECIMAL(38,10)", dm[0]["doris_type"] == "DECIMAL(38,10)", str(dm[0]))
    check("doris tinyint(1) -> BOOLEAN", dm[1]["doris_type"] == "BOOLEAN", str(dm[1]))
    check("doris datetime(3) -> DATETIMEV2(3)", dm[2]["doris_type"] == "DATETIMEV2(3)", str(dm[2]))

def test_schema_fields_and_mapping(nested_pkg, event_fields):
    """嵌套 message → HOCON row/array<{...}>；kafka 映射 nested → VARIANT"""
    fields = event_fields
    fmap = {f["name"]: f["st_type"] for f in fields}
    check("produce_time=bigint", fmap.get("produce_time") == "bigint")
    check("nested row type", "instrument_index" in fmap.get("depth_market_data", ""), fmap.get("depth_market_data", ""))
    check("repeated msg -> array<{...}>", "array<{" in fmap.get("depth_market_data", ""), fmap.get("depth_market_data", ""))

    # 4. 字段映射：kafka 嵌套 → VARIANT + nested 标记
    mapping = field_mapping.build_mapping("kafka", fields, variant_enabled=True)
    nested_cols = [m for m in mapping if m["nested"]]
    check("nested col -> VARIANT", all(m["doris_type"] == "VARIANT" for m in nested_cols), str(nested_cols))
    check("scalar col -> BIGINT", any(m["doris_type"] == "BIGINT" and m["doris_col"] == "produce_time" for m in mapping))

def test_render_kafka(db, nested_pkg, event_fields):
    """渲染管线：kafka-protobuf 作业渲染出完整 HOCON"""
    pkg = nested_pkg
    mapping = field_mapping.build_mapping("kafka", event_fields, variant_enabled=True)
    # 5. 渲染管线：kafka-protobuf 作业渲染出完整 HOCON
    ds = Datasource(env="dev", name="k1", type="kafka", connection={"servers": "kafka1:9092"})
    db.add(ds)
    db.commit()
    job = Job(
        name="smoke_kafka_job", env="dev", biz_line="md", source_type="kafka",
        datasource_id=ds.id, source_ref="to_doris",
        doris_db="seatunnel_sync", doris_table="md_kafka_event",
        proto_package_id=pkg.id, message_name="Event",
        field_mapping=mapping, options={},
    )
    job.datasource = ds
    job.proto_package = pkg
    db.add(job)
    db.commit()
    conf = render.render_conf(db, job)
    assert "format = protobuf" in conf
    assert "array<{" in conf
    assert 'table = "md_kafka_event"' in conf
    assert "kafka.config" in conf and "consumer.properties" not in conf
    print("----- 渲染结果（前 60 行）-----")
    print("\n".join(conf.splitlines()[:60]))
    check("render conf ok", True)
    # 与 SASL 渲染对照：PLAINTEXT 不应输出任何 SASL 配置
    check("PLAINTEXT: 不输出 security.protocol", "security.protocol" not in conf)

def test_render_kafka_sasl(db, nested_pkg, event_fields):
    """Kafka SASL 渲染：kafka.config 输出安全协议 + jaas（SCRAM），extra_config 逐行解析"""
    pkg = nested_pkg
    mapping = field_mapping.build_mapping("kafka", event_fields, variant_enabled=True)
    # 5b. Kafka SASL 渲染：kafka.config 输出安全协议 + jaas（SCRAM），extra_config 逐行解析
    ds_sasl = Datasource(env="dev", name="k_sasl", type="kafka", connection={
        "servers": "kafka1:9093",
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "SCRAM-SHA-512",
        "sasl_username": "user1",
        "sasl_password": encrypt('p@ss"w'),
        "extra_config": "client.id=vision-1\nmax.partition.fetch.bytes=10485760\nbadline",
    })
    db.add(ds_sasl)
    db.commit()
    job_sasl = Job(
        name="smoke_sasl_job", env="dev", biz_line="md", source_type="kafka",
        datasource_id=ds_sasl.id, source_ref="to_doris",
        doris_db="seatunnel_sync", doris_table="md_kafka_event_sasl",
        proto_package_id=pkg.id, message_name="Event",
        field_mapping=mapping, options={},
    )
    job_sasl.datasource = ds_sasl
    job_sasl.proto_package = pkg
    db.add(job_sasl)
    db.commit()
    conf_sasl = render.render_conf(db, job_sasl)

    in_kc = False
    for ln in conf_sasl.splitlines():
        if "kafka.config" in ln:
            in_kc = True
        if in_kc:
            print(ln)
            if ln.strip() == "}":
                break
    check("SASL: kafka.config 存在", "kafka.config" in conf_sasl)
    check("SASL: 无 consumer.properties", "consumer.properties" not in conf_sasl)
    check("SASL: security.protocol", "security.protocol = SASL_PLAINTEXT" in conf_sasl)
    check("SASL: sasl.mechanism", "sasl.mechanism = SCRAM-SHA-512" in conf_sasl)
    check("SASL: ScramLoginModule", "org.apache.kafka.common.security.scram.ScramLoginModule" in conf_sasl)
    check("SASL: jaas 用户名转义", 'username=\\"user1\\"' in conf_sasl)
    check("SASL: jaas 密码内双引号转义", 'password=\\"p@ss\\"w\\";' in conf_sasl)
    check("extra: 数字值不加引号", "max.partition.fetch.bytes = 10485760" in conf_sasl)
    check("extra: 字符串值加引号", 'client.id = "vision-1"' in conf_sasl)
    check("extra: 无 = 的坏行被跳过", "badline" not in conf_sasl)

def test_timestamp_columns(ts_mapping):
    """附加时间戳列：sink_only 不进 source schema；DDL 带 DEFAULT"""
    # 5c. 附加时间戳列：kafka_ts/doris_ts 均 sink_only 不进 source schema；
    # kafka_ts 由 Metadata transform 提取，doris_ts 走 Doris DEFAULT
    check("kafka_ts 追加", any(m["source"] == "kafka_ts" and m["doris_type"] == "DATETIMEV2(3)" for m in ts_mapping))
    etl = [m for m in ts_mapping if m["source"] == "doris_ts"]
    check("doris_ts 追加", len(etl) == 1 and etl[0].get("sink_only") and etl[0].get("default") == "CURRENT_TIMESTAMP(3)")
    schema_names = [f["name"] for f in field_mapping.mapping_to_schema_fields(ts_mapping)]
    check("kafka_ts 不进 source schema（sink_only，由 Metadata transform 补列）", "kafka_ts" not in schema_names)
    check("doris_ts 不进 source schema（sink_only）", "doris_ts" not in schema_names)
    # 非 kafka 源只加 doris_ts
    pg_ts = field_mapping.append_timestamp_columns([], "postgresql")
    check("非 kafka 源不加 kafka_ts", not any(m["source"] == "kafka_ts" for m in pg_ts)
          and any(m["source"] == "doris_ts" for m in pg_ts))
    # DDL：doris_ts 列带 DEFAULT CURRENT_TIMESTAMP（建表与加列一致）
    ddl = doris_ddl.build_create_table("seatunnel_sync", "t_ts", ts_mapping, True, 10)
    check("DDL 含 DEFAULT CURRENT_TIMESTAMP", "doris_ts` DATETIMEV2(3) DEFAULT CURRENT_TIMESTAMP(3)" in ddl, ddl.splitlines()[1] if ddl else "")

def test_ttl_ddl(ts_mapping):
    """TTL 动态分区建表：PARTITION BY RANGE + PROPERTIES + 分区列在 DUPLICATE KEY 最前"""
    # 5d. TTL 动态分区建表：PARTITION BY RANGE + PROPERTIES + 分区列在 DUPLICATE KEY 最前
    ttl = {"days": 30, "column": "doris_ts"}
    ddl_ttl = doris_ddl.build_create_table("seatunnel_sync", "t_ttl", ts_mapping, True, 10, 1, ttl)
    print("----- TTL 建表 SQL -----")
    print(ddl_ttl)
    check("TTL PARTITION BY RANGE", "PARTITION BY RANGE(`doris_ts`)()" in ddl_ttl)
    check("TTL start=-30", '"dynamic_partition.start" = "-30"' in ddl_ttl)
    check("TTL 列在 DUPLICATE KEY 最前", "DUPLICATE KEY(`doris_ts`," in ddl_ttl)
    check("TTL 与 replication_num 同一 PROPERTIES", '"replication_num" = "1"' in ddl_ttl
          and '"dynamic_partition.enable" = "true"' in ddl_ttl)
    ddl_plain = doris_ddl.build_create_table("seatunnel_sync", "t_plain", ts_mapping, True, 10, 1)
    check("无 TTL 不输出分区配置", "PARTITION BY" not in ddl_plain and "dynamic_partition" not in ddl_plain)

def test_unique_key_ddl(ts_mapping):
    """UNIQUE KEY 表模型：key 列规则与 DUPLICATE 相同"""
    ttl = {"days": 30, "column": "doris_ts"}
    # 5e. UNIQUE KEY 表模型：key 列规则与 DUPLICATE 相同（有 ttl 时 ttl 列最前）
    ddl_uq = doris_ddl.build_create_table("seatunnel_sync", "t_uq", ts_mapping, True, 10, 1, model="UNIQUE")
    check("UNIQUE KEY 生成", "UNIQUE KEY(`produce_time`," in ddl_uq and "DUPLICATE KEY" not in ddl_uq)
    ddl_uq_ttl = doris_ddl.build_create_table("seatunnel_sync", "t_uq2", ts_mapping, True, 10, 1, ttl, model="UNIQUE")
    check("UNIQUE + TTL 分区列最前", "UNIQUE KEY(`doris_ts`," in ddl_uq_ttl)

def test_flatten_render(db, nested_pkg):
    """嵌套 message 拍平 + SQL transform 渲染 + stream load columns 头"""
    pkg = nested_pkg
    job = _mk_kafka_job(db, pkg, "smoke_flat_job", [])
    # 5f. 嵌套 message 拍平：flattened_schema_fields + SQL transform 渲染
    flat_fields = proto_center.flattened_schema_fields(pkg, "Event", {"depth_market_data"})
    fbyname = {f["name"]: f for f in flat_fields}
    check("拍平标量叶子", fbyname.get("depth_market_data_instrument_index", {}).get("st_type") == "int"
          and fbyname["depth_market_data_instrument_index"].get("src_path") == "depth_market_data.instrument_index",
          str(fbyname.get("depth_market_data_instrument_index")))
    bb = fbyname.get("depth_market_data_bid_order_book", {})
    check("repeated 后代保持整列", "array<{" in bb.get("st_type", "")
          and bb.get("src_path") == "depth_market_data.bid_order_book", str(bb))
    check("未拍平字段无 src_path", "src_path" not in fbyname.get("produce_time", {}))
    flat_mapping = field_mapping.build_mapping("kafka", flat_fields, variant_enabled=True)
    check("拍平项透传 src_path", any(m.get("src_path") == "depth_market_data.instrument_index" for m in flat_mapping))
    check("schema 回根字段", any(f["name"] == "depth_market_data" and f["st_type"].startswith("{")
          for f in field_mapping.mapping_to_schema_fields(flat_mapping)))
    field_mapping.append_timestamp_columns(flat_mapping, "kafka")
    job.field_mapping = flat_mapping
    conf_flat = render.render_conf(db, job)
    check("conf 含 Sql transform", "Sql {" in conf_flat)
    sel = next((l.strip() for l in conf_flat.splitlines() if l.strip().startswith("SELECT")), "")
    print("  SELECT:", sel)
    check("SELECT 含拍平表达式", "depth_market_data.instrument_index AS depth_market_data_instrument_index" in sel)
    check("SELECT 不含嵌套整列", "depth_market_data," not in sel)
    check("SELECT 尾部 kafka_ts 原样透传（转换交给 Doris columns 头）",
          sel.endswith("kafka_ts FROM st_ts") and "CONCAT" not in sel, sel)
    check("Sql 输入为 st_ts（kafka_ts 启用）", 'plugin_input = "st_ts"' in conf_flat)
    check("sink 输入为 st_flat", 'plugin_input = "st_flat"' in conf_flat)

    # 5g. stream load columns 头：from_millisecond 转换 + jsonpaths 位置绑定
    check("conf 含 jsonpaths 头（转义形式）", 'jsonpaths = "[\\"$.produce_time\\"' in conf_flat, "")
    check("columns 头含 from_millisecond",
          "tmp_kafka_ts, kafka_ts = from_millisecond(tmp_kafka_ts)" in conf_flat, "")
    job.field_mapping = flat_mapping[:2]  # 去掉两个时间戳列（保留拍平项）
    conf_nohdr = render.render_conf(db, job)
    check("无 kafka_ts 时无 jsonpaths/columns 头", "jsonpaths" not in conf_nohdr and "from_millisecond" not in conf_nohdr)

def test_ms_epoch(db, nested_pkg):
    """TTL 选 BIGINT epoch 整数列：ms_epoch 标记 + 量级自适应 SQL transform"""
    flat_fields = proto_center.flattened_schema_fields(nested_pkg, "Event", {"depth_market_data"})
    # 5h. TTL 选 BIGINT epoch 整数列：ms_epoch 标记 + DATETIMEV2(3)
    # 量级自适应在 SeaTunnel SQL transform（CASE，ZetaSQLFunction 验证支持），
    # columns 头保持生产验证过的简单 from_millisecond 形式
    ms_mapping = field_mapping.build_mapping("kafka", flat_fields, variant_enabled=True)
    # 模拟 _collect_options 的处理：选 BIGINT 列（produce_time）作为 TTL 列
    ttl_col = next(m for m in ms_mapping if m["doris_col"] == "produce_time")
    ttl_col["ms_epoch"] = True
    ttl_col["doris_type"] = "DATETIMEV2(3)"
    jp, cols = render._doris_load_headers(ms_mapping, False)
    check("ms_epoch 列别名占位转换", cols[:2] == ["tmp_produce_time",
          "produce_time = from_millisecond(tmp_produce_time)"], str(cols[:2]))
    sel = render._sql_select(ms_mapping, False)
    check("ms_epoch 触发 SQL transform", sel is not None, str(sel))
    check("SQL transform 量级缩放 CASE",
          any("CASE WHEN produce_time >= 100000000000000000 THEN produce_time / 1000000" in e
              and e.endswith("END AS produce_time") for e in (sel or [])), str(sel))
    check("无 ms_epoch 无 flatten 不生成 SQL transform",
          render._sql_select([{"source": "a", "st_type": "bigint", "doris_col": "a",
                               "doris_type": "BIGINT", "nested": False}], False) is None)
    # mongo 作业带 ms_epoch：conf 渲染出 Sql transform 链（plugin_output/input 完整）
    mg_mapping = [{"source": "gen_time", "st_type": "bigint", "doris_col": "gen_time",
                   "doris_type": "DATETIMEV2(3)", "nested": False, "ms_epoch": True},
                  {"source": "symbol", "st_type": "string", "doris_col": "symbol",
                   "doris_type": "STRING", "nested": False}]
    mg_ds = Datasource(env="dev", name="mg_smoke", type="mongodb",
                       connection={"host": "mg:27017"})
    db.add(mg_ds)
    db.commit()
    mg_job = Job(name="smoke_mg_job", env="dev", biz_line="db1", source_type="mongodb",
                 datasource_id=mg_ds.id, source_ref="trading.OrderEvent",
                 doris_db="db1", doris_table="mg_t",
                 field_mapping=mg_mapping, options={})
    mg_job.datasource = mg_ds
    db.add(mg_job)
    db.commit()
    mg_conf = render.render_conf(db, mg_job)
    check("mongo conf 含 SQL transform CASE", "CASE WHEN gen_time >= 100000000000000000" in mg_conf)
    check("mongo conf transform 链完整", 'plugin_output = "st_src"' in mg_conf
          and 'plugin_input = "st_flat"' in mg_conf and 'plugin_output = "st_flat"' in mg_conf)
    check("mongo 批式 job.mode=BATCH", 'job.mode = "BATCH"' in mg_conf
          and "MongoDB-CDC" not in mg_conf)

    # mongo_mode=cdc：走 MongoDB-CDC 模板（STREAMING + tables_configs + startup.mode）
    cdc_job = Job(name="smoke_mg_cdc_job", env="dev", biz_line="db1", source_type="mongodb",
                  datasource_id=mg_ds.id, source_ref="trading.OrderEvent",
                  doris_db="db1", doris_table="mg_cdc_t",
                  field_mapping=mg_mapping,
                  options={"mongo_mode": "cdc", "cdc_startup_mode": "latest"})
    cdc_job.datasource = mg_ds
    db.add(cdc_job)
    db.commit()
    cdc_conf = render.render_conf(db, cdc_job)
    check("cdc job.mode=STREAMING", 'job.mode = "STREAMING"' in cdc_conf)
    check("cdc 源为 MongoDB-CDC", "MongoDB-CDC {" in cdc_conf)
    check("cdc tables_configs schema", 'table = "trading.OrderEvent"' in cdc_conf
          and '"gen_time" = "bigint"' in cdc_conf)
    check("cdc startup.mode", 'startup.mode = "latest"' in cdc_conf)
    check("cdc 默认 parallelism=1", "parallelism = 1" in cdc_conf)
    check("DUPLICATE 不开 enable-delete", "enable-delete" not in cdc_conf)
    cdc_job.options = {"mongo_mode": "cdc", "table_model": "UNIQUE"}
    cdc_conf_u = render.render_conf(db, cdc_job)
    check("UNIQUE+CDC 开 enable-delete", 'sink.enable-delete = "true"' in cdc_conf_u)
    check("无 kafka_ts 有 ms_epoch 也生成 jsonpaths", len(jp) == len(ms_mapping), str(jp))
    ddl_ms = doris_ddl.build_create_table("seatunnel_sync", "t_ms", ms_mapping, True, 10, 1,
                                          {"days": 30, "column": "produce_time"})
    check("ms_epoch 列 DDL 为 DATETIMEV2(3)", "`produce_time` DATETIMEV2(3)" in ddl_ms)
    check("PARTITION BY RANGE 用 epoch 列", "PARTITION BY RANGE(`produce_time`)()" in ddl_ms)

def test_pg_mapping():
    """PG 字段映射"""
    # 6. PG 字段映射
    pg_cols = [{"name": "id", "type": "int8"}, {"name": "price", "type": "numeric"},
               {"name": "info", "type": "jsonb"}, {"name": "created_at", "type": "timestamp"}]
    pg_map = field_mapping.build_mapping("postgresql", pg_cols, variant_enabled=True)
    expect = {"id": "BIGINT", "price": "DECIMAL(38,10)", "info": "VARIANT", "created_at": "DATETIME"}
    got = {m["doris_col"]: m["doris_type"] for m in pg_map}
    check("pg mapping", got == expect, str(got))

def test_hocon_escape_and_mask():
    """HOCON 转义（hq）与 conf 展示掩码（mask_conf）"""
    # 7. HOCON 转义（hq 过滤器）与 conf 展示掩码（mask_conf）
    check("hq 转义", render._hq('a"b\\c') == 'a\\"b\\\\c', render._hq('a"b\\c'))
    masked = _mask_conf('    password = "secret1"\n'
                        '    sasl.jaas.config = "x required username=\\"u\\" password=\\"p\\";"')
    check("conf 掩码 password 行", '"secret1"' not in masked and '"****"' in masked)
    check("conf 掩码 jaas 行", masked.count('"****"') == 2, masked)

def test_security_regressions(db):
    """URI 脱敏 / mongo URI 编码 / 映射与端口白名单 / WAL"""
    # 8. 安全修复回归：URI 脱敏 / mongo URI 编码 / 映射与端口白名单 / WAL

    check("URI 密码脱敏", "mongodb://u:****@h:27017/" in sanitize_error(
        "InvalidURI: mongodb://u:p%40ss@h:27017/?authSource=admin"))
    uri = mongo_d.build_uri({"host": "mg", "port": 27017, "username": "u@x", "password": "p@ss/w"})
    check("mongo URI 编码", "u%40x:p%40ss%2Fw@mg:27017" in uri, uri)

    conn, err = build_connection("postgresql", FormData(
        [("host", "pg"), ("port", "abc"), ("db", "x"), ("username", "u")]))
    check("端口非数字被拒", conn is None and "数字" in (err or ""), str(err))


    def _map_form(doris_type="BIGINT", default=""):
        return FormData([
            ("map_source", "a"), ("map_st_type", "bigint"), ("map_doris_col", "a"),
            ("map_doris_type", doris_type), ("map_nested", "0"), ("map_note", ""),
            ("map_sink_only", "0"), ("map_default", default),
            ("map_src_path", ""), ("map_src_root", ""), ("map_src_root_type", ""),
            ("map_enabled", "1"), ("map_flags", ""), ("map_agg", ""), ("map_ms_epoch", "0"),
        ])


    m, err = parse_mapping_form(_map_form("BIGINT) INVALID", ""))
    check("doris_type 注入被拒", m is None and bool(err), str(err))
    m, err = parse_mapping_form(_map_form("DATETIMEV2(3)", "CURRENT_TIMESTAMP(3)"))
    check("合法类型+默认值放行", m is not None and m[0]["doris_type"] == "DATETIMEV2(3)"
          and m[0]["default"] == "CURRENT_TIMESTAMP(3)")
    m, err = parse_mapping_form(_map_form("BIGINT", "'; DROP TABLE t; --"))
    check("default 注入被拒", m is None and bool(err), str(err))
    m, err = parse_mapping_form(_map_form("bigint", "'abc''d'"))
    check("小写自动大写+字面量默认值", m is not None and m[0]["doris_type"] == "BIGINT", str(m))
    m, err = parse_mapping_form(FormData(
        [("map_source", "a"), ("map_st_type", "bigint"), ("map_doris_col", "a"),
         ("map_doris_type", "DATETIMEV2(3)"), ("map_nested", "0"), ("map_note", ""),
         ("map_sink_only", "0"), ("map_default", ""),
         ("map_src_path", ""), ("map_src_root", ""), ("map_src_root_type", ""),
         ("map_enabled", "1"), ("map_flags", "ms_epoch"), ("map_agg", "")]))
    check("ms_epoch 经 map_flags 回传", err is None and m[0].get("ms_epoch") is True, f"{err}/{m}")

    check("SQLite WAL", db.execute(text("PRAGMA journal_mode")).scalar().lower() == "wal")

def test_ttl_options(ts_mapping):
    """TTL 增强：STRING 列按 DATE 存储 + ttl_num/ttl_unit 粒度 + 老 ttl_days 兼容"""
    # 9. TTL 增强：STRING 列按 DATE 存储 + ttl_num/ttl_unit 粒度 + 老 ttl_days 兼容
    str_mapping = [{"source": "trading_date", "st_type": "string", "doris_col": "trading_date",
                    "doris_type": "STRING", "nested": False}]
    opts, err = collect_options(
        FormData([("ttl_num", "6"), ("ttl_unit", "HOUR"), ("ttl_column", "trading_date")]), str_mapping)
    check("STRING 列选 TTL 改 DATE", err is None and str_mapping[0]["doris_type"] == "DATE", f"{err}/{str_mapping}")
    check("ttl_num/unit 存 options", opts == {"ttl_num": 6, "ttl_unit": "HOUR", "ttl_column": "trading_date"}, str(opts))
    ddl_hour = doris_ddl.build_create_table("seatunnel_sync", "t_h", ts_mapping, True, 10, 1,
                                            {"num": 6, "unit": "HOUR", "column": "doris_ts"})
    check("DDL time_unit=HOUR", '"dynamic_partition.time_unit" = "HOUR"' in ddl_hour)
    check("DDL start=-6", '"dynamic_partition.start" = "-6"' in ddl_hour)
    ddl_compat = doris_ddl.build_create_table("seatunnel_sync", "t_c", ts_mapping, True, 10, 1,
                                              {"days": 30, "column": "doris_ts"})
    check("老 ttl_days 兼容 DAY", '"dynamic_partition.time_unit" = "DAY"' in ddl_compat
          and '"dynamic_partition.start" = "-30"' in ddl_compat)

def test_row_flags_and_agg():
    """行级控制：enabled/flags/agg + UNIQUE/AGGREGATE key 规则"""
    # 10. 行级控制：enabled/flags/agg + UNIQUE/AGGREGATE key 规则
    def _map_form2():
        pairs = [
            ("map_source", "a"), ("map_source", "b"),
            ("map_st_type", "bigint"), ("map_st_type", "string"),
            ("map_doris_col", "a"), ("map_doris_col", "b"),
            ("map_doris_type", "BIGINT"), ("map_doris_type", "STRING"),
            ("map_nested", "0"), ("map_nested", "0"),
            ("map_note", ""), ("map_note", ""),
            ("map_sink_only", "0"), ("map_sink_only", "0"),
            ("map_default", ""), ("map_default", ""),
            ("map_src_path", ""), ("map_src_path", ""),
            ("map_src_root", ""), ("map_src_root", ""),
            ("map_src_root_type", ""), ("map_src_root_type", ""),
            ("map_enabled", "1"), ("map_enabled", "0"),
            ("map_flags", "key"), ("map_flags", ""),
            ("map_agg", "SUM"), ("map_agg", "REPLACE"),
            ("map_ms_epoch", "0"), ("map_ms_epoch", "0"),
        ]
        return FormData(pairs)


    m2, err = parse_mapping_form(_map_form2())
    check("enabled=0 行被丢弃", err is None and len(m2) == 1 and m2[0]["source"] == "a", f"{err}/{m2}")
    check("is_key/agg 解析", m2[0].get("is_key") is True and m2[0].get("agg") == "SUM", str(m2))

    agg_mapping = [
        {"source": "id", "st_type": "bigint", "doris_col": "id", "doris_type": "BIGINT",
         "nested": False, "is_key": True},
        {"source": "cnt", "st_type": "bigint", "doris_col": "cnt", "doris_type": "BIGINT",
         "nested": False, "agg": "SUM"},
        {"source": "note", "st_type": "string", "doris_col": "note", "doris_type": "STRING", "nested": False},
    ]
    ddl_key = doris_ddl.build_create_table("seatunnel_sync", "t_k", agg_mapping, True, 10, 1, model="UNIQUE")
    check("UNIQUE KEY 按 is_key 标记", "UNIQUE KEY(`id`)" in ddl_key, ddl_key.splitlines()[3])
    ddl_agg = doris_ddl.build_create_table("seatunnel_sync", "t_ag", agg_mapping, True, 10, 1, model="AGGREGATE")
    print("----- AGGREGATE 建表 SQL -----")
    print(ddl_agg)
    check("AGGREGATE KEY 生成", "AGGREGATE KEY(`id`)" in ddl_agg)
    check("非 key 列带聚合函数", "`cnt` BIGINT SUM" in ddl_agg)
    check("缺省聚合 REPLACE", "`note` STRING REPLACE" in ddl_agg)
    opts, err = collect_options(FormData([("table_model", "AGGREGATE")]),
                                 [{"doris_col": "v", "doris_type": "VARIANT", "nested": True}])
    check("AGGREGATE VARIANT 列报错", opts is None and "VARIANT" in (err or ""), str(err))

def test_key_prefix(ts_mapping):
    """key 列必须是表结构前缀：列定义按 key 顺序物理重排"""
    # 11. key 列必须是表结构前缀：列定义按 key 顺序物理重排（修复 Doris "ordered prefix" 报错）
    ddl_prefix = doris_ddl.build_create_table("seatunnel_sync", "t_p", ts_mapping, True, 10, 1,
                                              {"days": 30, "column": "doris_ts"})
    col_line = ddl_prefix.splitlines()[1].strip()
    key_line = next(l.strip() for l in ddl_prefix.splitlines() if l.strip().startswith("DUPLICATE KEY"))
    check("列定义以 ttl 列开头", col_line.startswith("`doris_ts`"), col_line[:80])
    check("KEY 前缀与列顺序一致", key_line.startswith("DUPLICATE KEY(`doris_ts`, `produce_time`, `kafka_ts`)"), key_line)
    rk = [{"source": "a", "st_type": "bigint", "doris_col": "a", "doris_type": "BIGINT", "nested": False},
          {"source": "b", "st_type": "bigint", "doris_col": "b", "doris_type": "BIGINT",
           "nested": False, "is_key": True}]
    ddl_rk = doris_ddl.build_create_table("seatunnel_sync", "t_rk", rk, True, 10, 1, model="UNIQUE")
    check("is_key 列前置", ddl_rk.splitlines()[1].strip().startswith("`b`") and "UNIQUE KEY(`b`)" in ddl_rk)

def test_ttl_props_parse(ts_mapping):
    """动态分区 SHOW CREATE 解析（ensure_table 核对已存在表 TTL）"""
    # 12. 动态分区解析（ensure_table 核对已存在表 TTL，避免"刚重建又告警 TTL 未生效"的误报）
    _SHOW_CREATE = '''CREATE TABLE `kafka_ipp` (\n `trading_date` date NULL\n) ENGINE=OLAP
    DUPLICATE KEY(`trading_date`)
    PARTITION BY RANGE(`trading_date`)()
    DISTRIBUTED BY HASH(`trading_date`) BUCKETS 3
    PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3"
    );'''
    p = doris_ddl._parse_ttl_props(_SHOW_CREATE)
    check("解析动态分区 start/unit", p == {"unit": "DAY", "start": -30,
          "create_history": False, "history_num": None}, str(p))
    p_h = doris_ddl._parse_ttl_props(_SHOW_CREATE.replace(
        '"dynamic_partition.end" = "3"',
        '"dynamic_partition.end" = "3",\n"dynamic_partition.create_history_partition" = "true",\n'
        '"dynamic_partition.history_partition_num" = "120"'))
    check("解析预建历史分区配置", p_h["create_history"] is True and p_h["history_num"] == 120, str(p_h))
    ddl_hn = doris_ddl.build_create_table("seatunnel_sync", "t_hn", ts_mapping, True, 10, 1,
                                          {"num": 7, "unit": "HOUR", "column": "doris_ts",
                                           "history_num": 120})
    check("建表带预建历史分区", '"dynamic_partition.create_history_partition" = "true"' in ddl_hn
          and '"dynamic_partition.history_partition_num" = "120"' in ddl_hn)
    check("未启用动态分区返回 None", doris_ddl._parse_ttl_props("CREATE TABLE t (a int)") is None)
    check("enable=false 返回 None",
          doris_ddl._parse_ttl_props(_SHOW_CREATE.replace('"true"', '"false"')) is None)
    # 与 ensure_table 判定一致：请求 30 DAY 与表一致 -> 不告警；不一致 -> 告警
    check("TTL 匹配判定", p["start"] == -30 and p["unit"] == "DAY")
