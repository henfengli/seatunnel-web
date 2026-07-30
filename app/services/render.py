"""conf 渲染管线：按 source_type 选 Jinja2 模板，渲染并留档 JobVersion。"""
from __future__ import annotations

import re
import urllib.parse

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..core.config import BASE_DIR
from ..core.crypto import decrypt_conn
from ..models import Job, JobVersion
from . import envs
from .field_mapping import mapping_to_schema_fields

_jinja = Environment(
    loader=FileSystemLoader(str(BASE_DIR / "app" / "templates" / "conf")),
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,  # partial 模板末尾换行需保留，否则 include 后与后续行粘连
)


def _hq(v) -> str:
    """HOCON 双引号字符串内容转义（反斜杠 + 双引号），防用户输入注入破坏 conf。"""
    return str(v if v is not None else "").replace("\\", "\\\\").replace('"', '\\"')


_jinja.filters["hq"] = _hq
# URL 百分号编码（Mongo URI 的用户名/密码段，含 @ : / # % ? 的密码必须编码，且先于 hq）
_jinja.filters["urlq"] = lambda v: urllib.parse.quote(str(v if v is not None else ""), safe="")

_TEMPLATES = {
    "kafka": "kafka_protobuf_to_doris.conf.j2",
    "postgresql": "postgresql_to_doris.conf.j2",
    "mongodb": "mongodb_to_doris.conf.j2",
    "doris": "doris_to_doris.conf.j2",
}


def _parse_extra_config(raw: str | None) -> list[str]:
    """kafka extra_config 逐行解析 key=value -> HOCON 行；无 = 的行跳过，非纯数字值加双引号。"""
    lines = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not key or not value:
            continue
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]  # 用户已加引号的去掉，统一由这里处理
        if not re.fullmatch(r"-?\d+(\.\d+)?", value):
            value = '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'
        lines.append(f"{key} = {value}")
    return lines


def _sql_select(mapping: list[dict], kafka_ts_enabled: bool) -> list[str] | None:
    """需要 SQL transform 时生成 SELECT 列表；不需要返回 None。

    触发条件（二选一）：
    - 拍平展开项（src_path）：`parent.child AS parent_child`（点号访问嵌套 ROW 子字段）；
    - ms_epoch 列：SeaTunnel SQL transform 原生支持 CASE（ZetaSQLFunction.executeCaseExpr），
      毫秒/微秒/纳秒按数值量级缩放到毫秒——Doris stream load columns 头只放简单
      `from_millisecond(col)`（生产验证过的形式），不放复杂表达式。
    kafka_ts 启用时原样透传（epoch 毫秒 → DATETIMEV2 的转换由 columns 头完成）。
    """
    if not any(m.get("src_path") for m in mapping) \
            and not any(m.get("ms_epoch") and not m.get("sink_only") for m in mapping):
        return None
    exprs = []
    for m in mapping:
        if m.get("sink_only"):
            continue
        if m.get("src_path"):
            exprs.append(f"{m['src_path']} AS {m['source']}")
        elif m.get("ms_epoch"):
            c = m["source"]
            exprs.append(
                f"CASE WHEN {c} >= 100000000000000000 THEN {c} / 1000000 "
                f"WHEN {c} >= 100000000000000 THEN {c} / 1000 ELSE {c} END AS {c}")
        else:
            exprs.append(m["source"])
    if kafka_ts_enabled:
        exprs.append("kafka_ts")
    return exprs


def _doris_load_headers(mapping: list[dict], kafka_ts_enabled: bool) -> tuple[list, list]:
    """生成 stream load 的 jsonpaths / columns 头（任一字段需要 from_millisecond 转换时）。

    触发条件：mapping 含 ms_epoch 标记列（BIGINT epoch 整数选作 TTL 列时打上）或 kafka_ts 启用。
    jsonpaths 按位置绑定 JSON 字段（列名与源字段名不一致也能正确映射）；
    ms_epoch 列与 kafka_ts 同走别名占位：tmp_col 占位 + from_millisecond(tmp_col)
    （epoch 量级缩放在 SeaTunnel SQL transform 完成，见 _sql_select）。转换要求 Doris 3.x+。
    """
    ms_fields = [m for m in mapping if m.get("ms_epoch") and not m.get("sink_only")]
    if not ms_fields and not kafka_ts_enabled:
        return [], []
    fields = [m for m in mapping if not m.get("sink_only")]
    # 注意：内层引号必须转义为 \"——模板外层还有一层 HOCON 字符串引号，
    # 不转义会得到 "[\"$.x\"...]" 的非法 HOCON（SeaTunnel 解析直接 500）
    jsonpaths = [f'\\"$.{m["source"]}\\"' for m in fields]
    # ms_epoch 列用别名占位 + 简单 from_millisecond(tmp_col)——与 kafka_ts 的
    # tmp_kafka_ts 同款（生产验证）；左右同名 from_millisecond(col) 会被 Doris 报
    # unknown reference column（NereidsStreamLoadTask 的列解析不支持自引用）
    columns = []
    for m in fields:
        if m.get("ms_epoch"):
            tmp = f"tmp_{m['doris_col']}"
            columns.append(tmp)
            columns.append(f"{m['doris_col']} = from_millisecond({tmp})")
        else:
            columns.append(m["doris_col"])
    if kafka_ts_enabled:
        jsonpaths.append('\\"$.kafka_ts\\"')
        columns += ["tmp_kafka_ts", "kafka_ts = from_millisecond(tmp_kafka_ts)"]
    return jsonpaths, columns


def _proto_schema_for(job: Job) -> str:
    """conf 里嵌入的 proto：裁剪为选中 message + 依赖闭包（解析异常时回退全量原文）。"""
    pkg = job.proto_package
    if not pkg or not pkg.content:
        return ""
    if not job.message_name:
        return pkg.content
    from . import proto_center

    try:
        return proto_center.subset_proto(pkg.content, job.message_name)
    except Exception:  # noqa: BLE001 - 裁剪失败回退全量，不阻塞渲染
        return pkg.content


def _mongo_hosts(ds: dict) -> str:
    """MongoDB-CDC 的 hosts：优先 host:port 拼接；uri 形式剥 scheme/凭据/路径。"""
    if ds.get("host"):
        return f"{ds['host']}:{ds.get('port', 27017)}"
    uri = ds.get("uri", "")
    m = re.match(r"mongodb(?:\+srv)?://(?:[^@/]+@)?([^/]+)", uri)
    return m.group(1) if m else uri


def render_conf(db: Session, job: Job) -> str:
    """按 job.source_type 渲染 SeaTunnel HOCON 配置文本。"""
    options = job.options
    mongo_cdc = job.source_type == "mongodb" and options.get("mongo_mode") == "cdc"
    source_db = source_table = ""
    if job.source_type != "kafka" and "." in job.source_ref:
        source_db, source_table = job.source_ref.split(".", 1)
    ds = decrypt_conn(job.datasource.connection)
    # kafka_ts（sink_only 标记）存在时，模板生成 Metadata transform 提取 kafka 时间戳
    kafka_ts_enabled = any(
        m.get("source") == "kafka_ts" and m.get("sink_only") for m in job.field_mapping
    )
    doris_jsonpaths, doris_columns = _doris_load_headers(job.field_mapping, kafka_ts_enabled)
    ctx = {
        "job_name": job.name,
        "parallelism": options.get("parallelism", 1),
        "checkpoint_interval": options.get("checkpoint_interval", 5000),
        "start_mode": options.get("start_mode", "latest"),
        "ds": ds,
        "mongo_hosts": _mongo_hosts(ds),
        "cdc_startup_mode": options.get("cdc_startup_mode", "initial"),
        "cdc_batch_size": int(options.get("cdc_batch_size", 1024)),
        "kafka_extra_config": _parse_extra_config(ds.get("extra_config", "")),
        "topic": job.source_ref if job.source_type == "kafka" else "",
        "message_name": job.message_name or "",
        "proto_content": _proto_schema_for(job),
        "schema_fields": mapping_to_schema_fields(job.field_mapping),
        "source_db": source_db,
        "source_table": source_table,
        "doris_db": job.doris_db,
        "doris_table": job.doris_table,
        "env": envs.get_env(db, job.env),
        "options": options,
        "kafka_ts_enabled": kafka_ts_enabled,
        # CDC 删除事件经 Doris delete sign 生效，仅 UNIQUE 模型支持（DUPLICATE 无该隐藏列）
        "enable_delete": mongo_cdc and options.get("table_model") == "UNIQUE",
        # 嵌套拍平：SQL transform 的 SELECT 列表（None = 无拍平，不生成 Sql transform）
        "sql_select": _sql_select(job.field_mapping, kafka_ts_enabled),
        # stream load jsonpaths/columns 头（ms_epoch 列或 kafka_ts 需要转换时非空）
        "doris_jsonpaths": doris_jsonpaths,
        "doris_columns": doris_columns,
    }
    template = "mongodb_cdc_to_doris.conf.j2" if mongo_cdc else _TEMPLATES[job.source_type]
    return _jinja.get_template(template).render(**ctx)


def render_and_save(db: Session, job: Job, note: str = "", created_by: str = "system") -> JobVersion:
    """渲染 conf -> 写 job.seatunnel_conf -> 新建 JobVersion 快照并返回。

    conf 含解密后的连接密码，落库一律加密存储（seatunnel_conf/JobVersion.conf），
    使用方经 crypto.decrypt_safe 还原（历史明文行兼容）。
    """
    from ..core.crypto import encrypt

    conf = render_conf(db, job)
    job.seatunnel_conf = encrypt(conf)
    max_version = (
        db.query(func.max(JobVersion.version)).filter(JobVersion.job_id == job.id).scalar() or 0
    )
    version = JobVersion(
        job_id=job.id,
        version=max_version + 1,
        conf=encrypt(conf),
        field_mapping=job.field_mapping,
        proto_version=job.proto_package.current_version if job.proto_package else None,
        note=note,
        created_by=created_by,
    )
    db.add(job)
    db.add(version)
    db.commit()
    db.refresh(version)
    return version
