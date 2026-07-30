"""页面路由：返回 HTML（Jinja2 渲染），表单提交与服务层编排操作。

约定：
- 普通表单提交 -> 303 重定向；htmx 请求 -> HX-Redirect 头（见 templating.goto）。
- 服务层返回 {"ok": False, "error": ...} 时，错误通过 query 参数 flash 到目标页。
- 密码等敏感字段只写不读：编辑表单留空表示不修改。
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..core.crypto import decrypt, encrypt, mask
from ..core.db import get_db
from ..models import DS_TYPES, JOB_STATUSES, BatchTask, Datasource, Environment, Job, JobEvent, MetricSample, ProtoPackage
from ..services import envs, health, orchestrator, proto_center, render
from ..services.field_mapping import append_timestamp_columns, build_mapping
from ..services.metadata import base as metadata
from ..templating import goto, templates

router = APIRouter()

_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
# Doris 类型白名单（防 map_doris_type 注入 DDL）：INT/VARCHAR(10)/DECIMAL(38,10)/DATETIMEV2(3) 等
_DORIS_TYPE_RE = re.compile(r"^[A-Z]+(V2)?(\(\d+(,\s*\d+)?\))?$")
# 列默认值白名单：CURRENT_TIMESTAMP[(3)] / 纯数字 / 单引号字符串字面量（内部单引号成双）
_DEFAULT_RE = re.compile(r"^(CURRENT_TIMESTAMP(\(3\))?|\d+|'([^']|'')*')$")


def _strip_proto_suffix(name: str) -> str:
    """用户输入的名称若带 .proto 后缀则去掉（页面上已暗示无需手动输入）。"""
    return name[:-6] if name.lower().endswith(".proto") else name


def _form_dict(form) -> dict:
    """Starlette FormData -> 普通 dict（仅保留 str 值），用于校验失败时回显。"""
    return {k: v for k, v in form.items() if isinstance(v, str)}


def _test_result_html(ok: bool, msg: str, label: str = "") -> str:
    """测试连接结果的内联片段（绿/红文字，不跳转）。"""
    from html import escape

    prefix = f"{escape(label)}: " if label else ""
    return f'<div class="{"test-ok" if ok else "test-fail"}">{"✓" if ok else "✗"} {prefix}{escape(msg)}</div>'


# ---------------------------------------------------------------- 总览

@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    env_cards = []
    for env in envs.list_envs(db):
        q = db.query(Job).filter(Job.env == env.name)
        h_status, h_title = health.env_aggregate(env.health_json)
        env_cards.append({
            "id": env.id,
            "name": env.name,
            "masters": [m.split("://")[-1] for m in envs.parse_masters(env.seatunnel_masters)],
            "fenodes": env.doris_fenodes,
            "job_count": q.count(),
            "running_count": q.filter(Job.status == "RUNNING").count(),
            "failed_count": q.filter(Job.status.in_(["FAILED", "ERROR"])).count(),
            "health": h_status,
            "health_title": h_title,
        })
    recent_events = (
        db.query(JobEvent).order_by(JobEvent.created_at.desc()).limit(10).all()
    )
    bad_jobs = db.query(Job).filter(Job.status.in_(["FAILED", "ERROR"])).all()
    stats = {
        "jobs": db.query(Job).count(),
        "running": db.query(Job).filter(Job.status == "RUNNING").count(),
        "failed": len(bad_jobs),
        "stopped": db.query(Job).filter(Job.status == "STOPPED").count(),
        "datasources": db.query(Datasource).count(),
        "protos": db.query(ProtoPackage).count(),
    }
    return templates.TemplateResponse(request, "dashboard.html", {
        "active": "dashboard",
        "env_cards": env_cards,
        "recent_events": recent_events,
        "bad_jobs": bad_jobs,
        "stats": stats,
    })


# ---------------------------------------------------------------- 环境

_ENV_NAME_RE = re.compile(r"^[A-Za-z0-9_\-]{1,32}$")


def _env_form_apply(e: Environment, form) -> str | None:
    """校验表单并填充环境字段；密码/Auth 留空 = 不修改。返回错误信息或 None。"""
    name = (form.get("name") or "").strip()
    if not _ENV_NAME_RE.match(name):
        return "名称必填，仅限字母/数字/下划线/中划线，最长 32 字符"
    masters = (form.get("seatunnel_masters") or "").strip()
    if not envs.parse_masters(masters):
        return "请填写至少一个 SeaTunnel master 地址（每行一个或逗号分隔）"
    fenodes = (form.get("doris_fenodes") or "").strip()
    if not fenodes:
        return "请填写 Doris fenodes"
    try:
        query_port = int((form.get("doris_query_port") or "9030").strip())
    except ValueError:
        return "Doris 查询端口必须是数字"
    username = (form.get("doris_username") or "").strip()
    if not username:
        return "请填写 Doris 用户名"
    try:
        buckets = int((form.get("default_buckets") or "10").strip())
    except ValueError:
        return "默认分桶数必须是数字"
    try:
        replication_num = int((form.get("replication_num") or "1").strip())
    except ValueError:
        return "副本数必须是数字"

    e.name = name
    e.seatunnel_masters = masters
    e.doris_fenodes = fenodes
    e.doris_query_port = query_port
    e.doris_username = username
    password = (form.get("doris_password") or "").strip()
    if password:
        e.doris_password = encrypt(password)
    e.variant_enabled = (form.get("variant_enabled") or "true").strip() == "true"
    e.default_buckets = buckets
    e.replication_num = replication_num
    e.proto_site_url = (form.get("proto_site_url") or "").strip() or None
    auth = (form.get("proto_site_auth") or "").strip()
    if auth:
        e.proto_site_auth = encrypt(auth)
    return None


@router.get("/environments", response_class=HTMLResponse)
def environment_list(request: Request, db: Session = Depends(get_db)):
    env_rows = []
    for env in envs.list_envs(db):
        status, title = health.env_aggregate(env.health_json)
        env_rows.append({
            "env": env,
            "masters": [m.split("://")[-1] for m in envs.parse_masters(env.seatunnel_masters)],
            "job_count": db.query(Job).filter(Job.env == env.name).count(),
            "health": status,
            "health_title": title,
            "parts": health.env_health_parts(env.health_json),
        })
    return templates.TemplateResponse(request, "environments.html", {
        "active": "environments", "env_rows": env_rows,
    })


@router.get("/environments/new", response_class=HTMLResponse)
def environment_new(request: Request):
    return templates.TemplateResponse(request, "environment_form.html", {
        "active": "environments", "env": None, "form": {}, "health": None, "error": None,
    })


@router.post("/environments")
async def environment_create(request: Request, db: Session = Depends(get_db)):
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "environment_form.html", {
            "active": "environments", "env": None, "form": _form_dict(form),
            "health": None, "error": msg,
        }, status_code=400)

    env = Environment()
    err = _env_form_apply(env, form)
    if err:
        return _err(err)
    db.add(env)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名环境: {env.name}")
    health.check_environment(db, env)  # 保存后自动测一次，失败不阻塞
    return goto(request, "/environments", f"环境 {env.name} 已创建")


@router.post("/environments/test-conn", response_class=HTMLResponse)
async def environment_test_conn(request: Request, db: Session = Depends(get_db)):
    """测试连接（不落库）：按表单原始字段构造临时配置测试，返回内联结果片段。"""
    form = await request.form()
    # 编辑时密码留空 = 用已存密码
    password = (form.get("doris_password") or "").strip()
    if not password:
        try:
            old = db.get(Environment, int(form.get("env_id") or 0))
        except (TypeError, ValueError):
            old = None
        if old is not None:
            password = decrypt(old.doris_password or "")
    try:
        query_port = int((form.get("doris_query_port") or "9030").strip())
    except ValueError:
        query_port = 9030
    env_dict = {
        "seatunnel": {"masters": envs.parse_masters(form.get("seatunnel_masters"))},
        "doris": {
            "fenodes": (form.get("doris_fenodes") or "").strip(),
            "query_port": query_port,
            "username": (form.get("doris_username") or "").strip(),
            "password": password,
        },
    }
    res = health.test_environment(env_dict)
    html = _test_result_html(*res["seatunnel"], label="SeaTunnel")
    html += _test_result_html(*res["doris"], label="Doris")
    return HTMLResponse(html)


@router.post("/environments/{env_id}/retest")
def environment_retest(request: Request, env_id: int, db: Session = Depends(get_db)):
    """重新测试已存环境并落库，跳回编辑页。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    health.check_environment(db, env)
    status, _ = health.env_aggregate(env.health_json)
    ok = status == "ok"
    return goto(request, f"/environments/{env_id}/edit",
                "连接测试通过" if ok else "连接测试未通过，详见健康状态", ok=ok)


@router.get("/environments/{env_id}/logs", response_class=HTMLResponse)
def environment_logs(request: Request, env_id: int, tail: int = 500,
                     db: Session = Depends(get_db)):
    """环境级引擎主日志页（SeaTunnel 主日志截尾；作业日志在作业详情页看）。"""
    from ..services import monitor as mon

    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    return templates.TemplateResponse(request, "environment_logs.html", {
        "active": "environments", "env": env,
        "logs": mon.engine_logs(db, env.name, min(max(tail, 50), 2000)),
    })


@router.get("/environments/{env_id}/edit", response_class=HTMLResponse)
def environment_edit(request: Request, env_id: int, db: Session = Depends(get_db)):
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    return templates.TemplateResponse(request, "environment_form.html", {
        "active": "environments", "env": env, "form": {},
        "health": health.env_aggregate(env.health_json), "error": None,
    })


@router.post("/environments/{env_id}")
async def environment_update(request: Request, env_id: int, db: Session = Depends(get_db)):
    """更新环境；密码/Auth 留空表示不修改。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "environment_form.html", {
            "active": "environments", "env": env, "form": _form_dict(form),
            "health": health.env_aggregate(env.health_json), "error": msg,
        }, status_code=400)

    old_name = env.name
    err = _env_form_apply(env, form)
    if err:
        return _err(err)
    # 有数据源/作业引用时拒绝改名（引用按环境名悬空）
    if env.name != old_name:
        ds_cnt = db.query(Datasource).filter(Datasource.env == old_name).count()
        job_cnt = db.query(Job).filter(Job.env == old_name).count()
        if ds_cnt or job_cnt:
            db.rollback()
            return _err(f"有 {ds_cnt} 个数据源、{job_cnt} 个作业引用环境 {old_name}，"
                        f"不能改名；如需改名请先删除引用")
    db.add(env)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名环境: {env.name}")
    health.check_environment(db, env)  # 保存后自动测一次，失败不阻塞
    return goto(request, "/environments", f"环境 {env.name} 已更新")


@router.delete("/environments/{env_id}")
def environment_delete(request: Request, env_id: int, db: Session = Depends(get_db)):
    """删除环境；存在关联数据源或作业时拒绝。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    ds_cnt = db.query(Datasource).filter(Datasource.env == env.name).count()
    job_cnt = db.query(Job).filter(Job.env == env.name).count()
    if ds_cnt or job_cnt:
        return goto(request, "/environments",
                    f"环境 {env.name} 存在关联数据源 {ds_cnt} 个、作业 {job_cnt} 个，无法删除",
                    ok=False)
    db.delete(env)
    db.commit()
    return goto(request, "/environments", f"环境 {env.name} 已删除")


# ---------------------------------------------------------------- 数据源

# 各类型的必填连接字段（密码单独处理，加密存储）
_DS_REQUIRED = {
    "kafka": ["servers"],
    "mongodb": ["host", "port", "username"],
    "postgresql": ["host", "port", "db", "username"],
    "doris": ["fenodes", "query_port", "username"],
}

# Kafka 安全协议 / SASL 机制可选项
_KAFKA_PROTOCOLS = ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL")
_KAFKA_MECHANISMS = ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")


def _build_connection(ds_type: str, form) -> tuple[dict | None, str | None]:
    """按类型从表单构建 connection dict；返回 (conn, 错误信息)。空密码 = 不修改。"""
    conn: dict = {}
    for key in _DS_REQUIRED[ds_type]:
        val = (form.get(key) or "").strip()
        if not val:
            return None, f"缺少必填连接字段: {key}"
        conn[key] = val
    # 端口类字段必须是数字
    for key in ("port", "query_port"):
        if key in conn and not conn[key].isdigit():
            return None, f"端口字段 {key} 必须是数字"
    if ds_type == "mongodb" and (form.get("auth_db") or "").strip():
        conn["auth_db"] = form["auth_db"].strip()
    if ds_type == "kafka":
        protocol = (form.get("security_protocol") or "PLAINTEXT").strip()
        if protocol not in _KAFKA_PROTOCOLS:
            return None, f"非法安全协议: {protocol}"
        conn["security_protocol"] = protocol
        if protocol.startswith("SASL"):
            mechanism = (form.get("sasl_mechanism") or "PLAIN").strip()
            if mechanism not in _KAFKA_MECHANISMS:
                return None, f"非法 SASL 机制: {mechanism}"
            conn["sasl_mechanism"] = mechanism
            conn["sasl_username"] = (form.get("sasl_username") or "").strip()
            sasl_password = (form.get("sasl_password") or "").strip()
            if sasl_password:
                conn["sasl_password"] = encrypt(sasl_password)
        extra_config = (form.get("extra_config") or "").strip()
        if extra_config:
            conn["extra_config"] = extra_config
    password = (form.get("password") or "").strip()
    if password:
        conn["password"] = encrypt(password)
    return conn, None


@router.get("/datasources", response_class=HTMLResponse)
def datasource_list(request: Request, db: Session = Depends(get_db)):
    ds_list = db.query(Datasource).order_by(Datasource.env, Datasource.id).all()
    return templates.TemplateResponse(request, "datasources.html", {
        "active": "datasources", "ds_list": ds_list,
    })


@router.get("/datasources/new", response_class=HTMLResponse)
def datasource_new(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "datasource_form.html", {
        "active": "datasources", "ds": None, "conn": {},
        "env_names": envs.env_names(db), "ds_types": DS_TYPES, "error": None,
    })


@router.post("/datasources")
async def datasource_create(request: Request, db: Session = Depends(get_db)):
    """创建数据源（普通表单提交）；校验失败回显表单。"""
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "datasource_form.html", {
            "active": "datasources", "ds": None, "conn": _form_dict(form),
            "env_names": envs.env_names(db), "ds_types": DS_TYPES,
            "error": msg,
        }, status_code=400)

    env = (form.get("env") or "").strip()
    name = (form.get("name") or "").strip()
    ds_type = (form.get("type") or "").strip()
    if env not in envs.env_names(db):
        return _err(f"非法环境: {env or '(空)'}")
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    if ds_type not in DS_TYPES:
        return _err(f"非法数据源类型: {ds_type or '(空)'}")
    conn, err = _build_connection(ds_type, form)
    if err:
        return _err(err)
    # 允许空密码（如本地 dev 环境），但 key 始终存在，结构统一
    conn.setdefault("password", "")

    ds = Datasource(env=env, name=name, type=ds_type,
                    connection_json=Datasource._dumps(conn))
    db.add(ds)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"环境 {env} 下已存在同名数据源: {name}")
    health.check_datasource(db, ds)  # 保存后自动测一次，失败不阻塞
    metadata.refresh(db, ds)  # 创建后立即自动刷新元数据（之后由 watchdog 每日刷新）
    if ds.metadata_status == "ok":
        return goto(request, f"/datasources/{ds.id}",
                    f"数据源 {name} 已创建，元数据已自动刷新")
    return goto(request, f"/datasources/{ds.id}",
                f"数据源 {name} 已创建，但元数据自动刷新失败: {ds.metadata_error}", ok=False)


@router.post("/datasources/test-conn", response_class=HTMLResponse)
async def datasource_test_conn(request: Request, db: Session = Depends(get_db)):
    """测试连接（不落库）：按表单原始字段构造临时连接测试，返回内联结果片段。"""
    form = await request.form()
    ds_type = (form.get("type") or "").strip()
    if ds_type not in DS_TYPES:
        return HTMLResponse(_test_result_html(False, f"未知数据源类型: {ds_type or '(空)'}"))
    try:
        old = db.get(Datasource, int(form.get("ds_id") or 0))
    except (TypeError, ValueError):
        old = None
    conn, err = _build_connection(ds_type, form)
    if err:
        return HTMLResponse(_test_result_html(False, err))
    if old is not None:  # 编辑时密码留空 = 用已存密码
        conn.setdefault("password", old.connection.get("password", ""))
        conn.setdefault("sasl_password", old.connection.get("sasl_password", ""))
    ok, msg = health.test_datasource(ds_type, health.decrypted(conn))
    return HTMLResponse(_test_result_html(ok, msg))


@router.get("/datasources/{ds_id}", response_class=HTMLResponse)
def datasource_detail(request: Request, ds_id: int, db: Session = Depends(get_db)):
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    # 密码掩码展示，绝不明文回传
    conn_display = {}
    for k, v in ds.connection.items():
        if isinstance(v, str) and v and "password" in k.lower():
            try:
                conn_display[k] = mask(decrypt(v)) or "(空)"
            except Exception:  # noqa: BLE001
                conn_display[k] = "****"
        else:
            conn_display[k] = v
    md = ds.metadata_dict
    return templates.TemplateResponse(request, "datasource_detail.html", {
        "active": "datasources", "ds": ds, "conn_display": conn_display,
        "md": md,
    })


@router.get("/datasources/{ds_id}/edit", response_class=HTMLResponse)
def datasource_edit(request: Request, ds_id: int, db: Session = Depends(get_db)):
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    return templates.TemplateResponse(request, "datasource_form.html", {
        "active": "datasources", "ds": ds, "conn": ds.connection,
        "env_names": envs.env_names(db), "ds_types": DS_TYPES, "error": None,
    })


@router.post("/datasources/{ds_id}")
async def datasource_update(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """更新数据源；类型不可改，密码留空表示不修改。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "datasource_form.html", {
            "active": "datasources", "ds": ds,
            "conn": {**ds.connection, **_form_dict(form)},
            "env_names": envs.env_names(db), "ds_types": DS_TYPES,
            "error": msg,
        }, status_code=400)

    env = (form.get("env") or "").strip()
    name = (form.get("name") or "").strip()
    if env not in envs.env_names(db):
        return _err(f"非法环境: {env or '(空)'}")
    if env != ds.env and db.query(Job).filter(Job.datasource_id == ds.id).count():
        return _err("该数据源有作业引用，不能修改所属环境（防止跨环境错配）")
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    conn, err = _build_connection(ds.type, form)
    if err:
        return _err(err)
    conn.setdefault("password", ds.connection.get("password", ""))  # 留空 = 保留原密码
    if ds.type == "kafka":
        conn.setdefault("sasl_password", ds.connection.get("sasl_password", ""))

    ds.env, ds.name = env, name
    ds.connection_json = Datasource._dumps(conn)
    db.add(ds)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"环境 {env} 下已存在同名数据源: {name}")
    health.check_datasource(db, ds)  # 保存后自动测一次，失败不阻塞
    metadata.refresh(db, ds)  # 连接信息可能变了，立即自动刷新元数据
    if ds.metadata_status == "ok":
        return goto(request, f"/datasources/{ds.id}",
                    f"数据源 {name} 已更新，元数据已自动刷新")
    return goto(request, f"/datasources/{ds.id}",
                f"数据源 {name} 已更新，但元数据自动刷新失败: {ds.metadata_error}", ok=False)


@router.post("/datasources/{ds_id}/retest")
def datasource_retest(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """重新测试已存数据源并落库，跳回详情页。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    health.check_datasource(db, ds)
    ok = ds.health_status == "ok"
    return goto(request, f"/datasources/{ds_id}",
                f"连接测试通过：{ds.health_detail}" if ok
                else f"连接测试失败：{ds.health_detail}", ok=ok)


@router.delete("/datasources/{ds_id}")
def datasource_delete(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """删除数据源；存在关联作业时拒绝。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    job_cnt = db.query(Job).filter(Job.datasource_id == ds_id).count()
    if job_cnt:
        return goto(request, "/datasources",
                    f"数据源 {ds.name} 被 {job_cnt} 个作业引用，无法删除", ok=False)
    db.delete(ds)
    db.commit()
    return goto(request, "/datasources", f"数据源 {ds.name} 已删除")


@router.post("/datasources/{ds_id}/refresh")
def datasource_refresh(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """手动刷新元数据，重定向回详情页。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    metadata.refresh(db, ds)
    if ds.metadata_status == "ok":
        return goto(request, f"/datasources/{ds_id}", "元数据刷新成功")
    return goto(request, f"/datasources/{ds_id}",
                f"元数据刷新失败: {ds.metadata_error}", ok=False)


# ---------------------------------------------------------------- Proto 包

@router.get("/protos", response_class=HTMLResponse)
def proto_list(request: Request, db: Session = Depends(get_db)):
    pkgs = db.query(ProtoPackage).order_by(ProtoPackage.id).all()
    return templates.TemplateResponse(request, "protos.html", {
        "active": "protos", "pkgs": pkgs,
    })


@router.get("/protos/new", response_class=HTMLResponse)
def proto_new(request: Request):
    return templates.TemplateResponse(request, "proto_form.html", {
        "active": "protos", "pkg": None, "error": None, "form": {},
    })


@router.post("/protos")
async def proto_create(request: Request, db: Session = Depends(get_db)):
    """创建 proto 包：粘贴内容走 update_content，否则填了 source_url 走首次拉取。"""
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "proto_form.html", {
            "active": "protos", "pkg": None, "error": msg, "form": _form_dict(form),
        }, status_code=400)

    name = _strip_proto_suffix((form.get("name") or "").strip())
    source_url = (form.get("source_url") or "").strip()
    content = (form.get("content") or "").strip()
    auth_header = (form.get("auth_header") or "").strip()
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    if not source_url and not content:
        return _err("请填写来源 URL 或直接粘贴 proto 内容")
    try:
        poll_interval = max(60, int(form.get("poll_interval_sec") or 3600))
    except ValueError:
        return _err("拉取间隔必须是数字（秒）")

    pkg = ProtoPackage(
        name=name, source_url=source_url, poll_interval_sec=poll_interval,
        auth_header=encrypt(auth_header) if auth_header else "",
        origin="paste" if content else "url",
    )
    db.add(pkg)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名 proto 包: {name}")

    # 首次载入内容：优先粘贴内容，其次 URL 拉取
    if content:
        proto_center.update_content(db, pkg, content)
    else:
        proto_center.poll_package(db, pkg)
    if pkg.status == "error":
        return goto(request, f"/protos/{pkg.id}",
                    f"proto 包已创建，但首次载入失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg.id}", f"proto 包 {name} 已创建")


@router.post("/protos/upload")
async def proto_upload(request: Request, db: Session = Depends(get_db),
                       files: list[UploadFile] = File(...)):
    """批量上传 .proto 文件：每个文件建一个 proto 包（名称取文件名去后缀）。"""
    created, skipped, failed = [], [], []
    for f in files:
        name = _strip_proto_suffix((f.filename or "").rsplit("/", 1)[-1].strip())
        if not name or not _NAME_RE.match(name):
            failed.append(f"{f.filename}（文件名不合法）")
            continue
        if db.query(ProtoPackage).filter(ProtoPackage.name == name).first():
            skipped.append(name)
            continue
        try:
            content = (await f.read()).decode("utf-8")
        except UnicodeDecodeError:
            failed.append(f"{f.filename}（非 UTF-8 文本）")
            continue
        pkg = ProtoPackage(name=name, source_url="", origin="upload")
        db.add(pkg)
        db.commit()
        proto_center.update_content(db, pkg, content)
        if pkg.status == "error":
            failed.append(f"{name}（解析失败: {pkg.error}）")
        else:
            created.append(name)
    parts = []
    if created:
        parts.append(f"成功 {len(created)} 个: {', '.join(created)}")
    if skipped:
        parts.append(f"跳过同名 {len(skipped)} 个: {', '.join(skipped)}")
    if failed:
        parts.append(f"失败 {len(failed)} 个: {', '.join(failed)}")
    return goto(request, "/protos", "；".join(parts) or "未选择文件",
                ok=not (failed and not created))


@router.get("/protos/{pkg_id}", response_class=HTMLResponse)
def proto_detail(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    diff = json.loads(pkg.diff_json) if pkg.diff_json else None
    parsed = pkg.parsed or {}
    return templates.TemplateResponse(request, "proto_detail.html", {
        "active": "protos", "pkg": pkg, "diff": diff,
        "messages": parsed.get("messages", {}),
        "top_level": parsed.get("top_level", []),
    })


@router.get("/protos/{pkg_id}/edit", response_class=HTMLResponse)
def proto_edit(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    return templates.TemplateResponse(request, "proto_form.html", {
        "active": "protos", "pkg": pkg, "error": None, "form": {},
    })


@router.post("/protos/{pkg_id}")
async def proto_update(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """更新 proto 包基本信息；auth_header 留空不修改；粘贴内容则重新解析。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    form = await request.form()

    def _err(msg: str):
        return templates.TemplateResponse(request, "proto_form.html", {
            "active": "protos", "pkg": pkg, "error": msg, "form": {},
        }, status_code=400)

    name = _strip_proto_suffix((form.get("name") or "").strip())
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    try:
        poll_interval = max(60, int(form.get("poll_interval_sec") or 3600))
    except ValueError:
        return _err("拉取间隔必须是数字（秒）")

    pkg.name = name
    old_url = pkg.source_url
    pkg.source_url = (form.get("source_url") or "").strip()
    if pkg.source_url and pkg.source_url != old_url:
        pkg.origin = "url"  # 改了来源 URL 则回到 URL 拉取模式（粘贴内容保存时仍会覆盖为 paste）
    pkg.poll_interval_sec = poll_interval
    auth_header = (form.get("auth_header") or "").strip()
    if auth_header:
        pkg.auth_header = encrypt(auth_header)
    db.add(pkg)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名 proto 包: {name}")

    content = (form.get("content") or "").strip()
    if content:
        pkg.origin = "paste"  # 粘贴了新内容则来源方式变为手动粘贴
        proto_center.update_content(db, pkg, content)
        if pkg.status == "error":
            return goto(request, f"/protos/{pkg.id}",
                        f"基本信息已保存，但 proto 解析失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg.id}", f"proto 包 {name} 已更新")


@router.post("/protos/{pkg_id}/poll")
def proto_poll(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """立即从 source_url 拉取最新 proto。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    if not pkg.source_url:
        return goto(request, f"/protos/{pkg_id}", "该 proto 包未配置来源 URL", ok=False)
    proto_center.poll_package(db, pkg)
    if pkg.status == "error":
        return goto(request, f"/protos/{pkg_id}", f"拉取失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg_id}", f"拉取完成，状态: {pkg.status}")


@router.delete("/protos/{pkg_id}")
def proto_delete(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """删除 proto 包；有作业引用时拒绝并列出引用作业名。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    refs = db.query(Job).filter(Job.proto_package_id == pkg_id).all()
    if refs:
        names = "、".join(j.name for j in refs)
        return goto(request, "/protos",
                    f"proto 包 {pkg.name} 被 {len(refs)} 个作业引用（{names}），无法删除", ok=False)
    db.delete(pkg)
    db.commit()
    return goto(request, "/protos", f"proto 包 {pkg.name} 已删除")


# ---------------------------------------------------------------- 作业

@router.get("/jobs", response_class=HTMLResponse)
def job_list(request: Request, env: str = "", tag: str = "", status: str = "",
             db: Session = Depends(get_db)):
    q = db.query(Job)
    if env:
        q = q.filter(Job.env == env)
    if status:
        q = q.filter(Job.status == status)
    if tag:
        q = q.filter(Job.tags.contains(tag.strip()))
    jobs = q.order_by(Job.updated_at.desc()).all()
    return templates.TemplateResponse(request, "jobs.html", {
        "active": "jobs", "jobs": jobs,
        "env_names": envs.env_names(db), "statuses": JOB_STATUSES,
        "f_env": env, "f_tag": tag, "f_status": status,
    })


@router.post("/jobs/batch")
async def job_batch_action(request: Request, db: Session = Depends(get_db)):
    """批量操作入口：勾选作业 + action -> 建 BatchTask 后台执行，跳进度页。"""
    from ..services import batch_ops

    form = await request.form()
    action = (form.get("action") or "").strip()
    ids: list[int] = []
    for raw in form.getlist("job_ids"):
        try:
            ids.append(int(raw))
        except (TypeError, ValueError):
            pass
    if action not in batch_ops.ACTIONS:
        return goto(request, "/jobs", f"非法批量操作: {action or '(空)'}", ok=False)
    if not ids:
        return goto(request, "/jobs", "请先勾选作业", ok=False)
    jobs = db.query(Job).filter(Job.id.in_(ids)).order_by(Job.id).all()
    if not jobs:
        return goto(request, "/jobs", "勾选的作业不存在", ok=False)

    params: dict = {}
    if action == "options":
        for k in batch_ops.INT_OPTION_KEYS:
            raw = (form.get(k) or "").strip()
            if raw:
                try:
                    params[k] = int(raw)
                except ValueError:
                    return goto(request, "/jobs", f"批量配置 {k} 必须是整数", ok=False)
        for k in batch_ops.STR_OPTION_KEYS:
            raw = (form.get(k) or "").strip()
            if raw:
                params[k] = raw
        tags = (form.get("tags") or "").strip()
        if tags:
            params["tags"] = tags
        if (form.get("restart") or "").strip() == "on":
            params["restart"] = True
        if not any(k != "restart" for k in params):
            return goto(request, "/jobs", "批量改配置：至少填写一个要修改的字段", ok=False)

    task = batch_ops.create_batch(db, action, jobs, params)
    batch_ops.start_batch(task.id)
    return goto(request, f"/batch/{task.id}",
                f"批量任务 #{task.id} 已启动（{action}，共 {task.total} 个作业）")


@router.get("/batch/{task_id}", response_class=HTMLResponse)
def batch_detail(request: Request, task_id: int, db: Session = Depends(get_db)):
    """批量任务进度页：执行中每 2 秒自动刷新。"""
    task = db.get(BatchTask, task_id)
    if not task:
        return goto(request, "/jobs", "批量任务不存在", ok=False)
    return templates.TemplateResponse(request, "batch_detail.html", {
        "active": "jobs", "task": task,
    })


@router.get("/jobs/new", response_class=HTMLResponse)
def job_new(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse(request, "job_form.html", {
        "active": "jobs", "env_names": envs.env_names(db),
        "ds_types": DS_TYPES, "error": None, "form": {},
        "job": None, "mapping": [], "joptions": {}, "add_ts": True,
    })


def _parse_mapping_form(form, prefix: str = "") -> tuple[list[dict] | None, str | None]:
    """解析预览表回传的 map_* 字段为 mapping；未回传返回 (None, None)，非法返回 (None, 错误)。

    prefix 供批量建作业按对象命名空间解析（如 "o3_map_source"）。
    """
    sources = form.getlist(f"{prefix}map_source")
    if not sources:
        return None, None
    st_types = form.getlist(f"{prefix}map_st_type")
    doris_cols = form.getlist(f"{prefix}map_doris_col")
    doris_types = form.getlist(f"{prefix}map_doris_type")
    nesteds = form.getlist(f"{prefix}map_nested")
    notes = form.getlist(f"{prefix}map_note")
    sink_onlys = form.getlist(f"{prefix}map_sink_only")
    defaults = form.getlist(f"{prefix}map_default")
    src_paths = form.getlist(f"{prefix}map_src_path")
    src_roots = form.getlist(f"{prefix}map_src_root")
    src_root_types = form.getlist(f"{prefix}map_src_root_type")
    enableds = form.getlist(f"{prefix}map_enabled")
    flags_list = form.getlist(f"{prefix}map_flags")
    aggs = form.getlist(f"{prefix}map_agg")
    if not (len(sources) == len(st_types) == len(doris_cols)
            == len(doris_types) == len(nesteds) == len(notes)
            == len(sink_onlys) == len(defaults) == len(src_paths)
            == len(src_roots) == len(src_root_types) == len(enableds)
            == len(flags_list) == len(aggs)):
        return None, "字段映射不完整，请重新生成映射预览"
    mapping = []
    for i in range(len(sources)):
        if enableds[i] != "1":
            continue  # 行级「启用」未勾选：该字段不进作业，直接丢弃
        if not _IDENT_RE.match(doris_cols[i]):
            return None, f"Doris 列名非法: {doris_cols[i]}"
        doris_type = doris_types[i].strip().upper() or "STRING"
        if not _DORIS_TYPE_RE.match(doris_type):
            return None, f"Doris 类型非法（仅允许类型名+可选长度/精度）: {doris_types[i]}"
        if not re.match(r"^[\w<>{}:,\- ]+$", st_types[i]):
            return None, f"SeaTunnel 类型非法: {st_types[i]}"
        item = {
            "source": sources[i], "st_type": st_types[i],
            "doris_col": doris_cols[i], "doris_type": doris_type,
            "nested": nesteds[i] == "1",
        }
        if notes[i]:
            item["note"] = notes[i]
        if sink_onlys[i] == "1":
            item["sink_only"] = True
        default = defaults[i].strip()
        if default:
            if not _DEFAULT_RE.match(default):
                return None, f"列默认值只允许 CURRENT_TIMESTAMP/CURRENT_TIMESTAMP(3)/数字/单引号字符串: {default}"
            item["default"] = default
        if src_paths[i]:
            item["src_path"] = src_paths[i]
            item["src_root"] = src_roots[i]
            item["src_root_type"] = src_root_types[i]
        # 行级标记（逗号分隔）：key（UNIQUE/AGGREGATE 的 key 列）、ms_epoch（epoch 毫秒 TTL 列）
        flags = flags_list[i].split(",")
        if "key" in flags:
            item["is_key"] = True
        if "ms_epoch" in flags:
            item["ms_epoch"] = True
        agg = aggs[i].strip().upper()
        if agg:
            if agg not in ("REPLACE", "REPLACE_IF_NOT_NULL", "SUM", "MIN", "MAX",
                           "HLL_UNION", "BITMAP_UNION"):
                return None, f"非法聚合函数: {agg}"
            item["agg"] = agg
        mapping.append(item)
    if not mapping:
        return None, "字段映射不能为空（至少启用一个字段）"
    cols = [m["doris_col"] for m in mapping]
    if len(set(cols)) != len(cols):
        dup = next(c for c in cols if cols.count(c) > 1)
        return None, f"Doris 列名重复: {dup}"
    return mapping, None


def _shared_options(form) -> tuple[dict | None, str | None]:
    """高级选项中的作业级共享部分（并行度/checkpoint/批大小/起始位点等）。"""
    options: dict = {}
    for key in ("parallelism", "checkpoint_interval", "fetch_max_bytes",
                "max_poll_records", "buckets"):
        raw = (form.get(key) or "").strip()
        if raw:
            try:
                options[key] = int(raw)
            except ValueError:
                return None, f"高级选项 {key} 必须是整数"
            if options[key] < 1:
                return None, f"高级选项 {key} 必须 >= 1"
    if (form.get("start_mode") or "").strip():
        options["start_mode"] = form["start_mode"].strip()
    if (form.get("consumer_group") or "").strip():
        options["consumer_group"] = form["consumer_group"].strip()
    # MongoDB 同步模式：批式快照（默认，一次性全量）/ CDC（持续同步，需副本集）
    if (form.get("mongo_mode") or "").strip() == "cdc":
        options["mongo_mode"] = "cdc"
        if (form.get("cdc_startup_mode") or "").strip() == "latest":
            options["cdc_startup_mode"] = "latest"
        raw = (form.get("cdc_batch_size") or "").strip()
        if raw:
            try:
                options["cdc_batch_size"] = int(raw)
            except ValueError:
                return None, "高级选项 cdc_batch_size 必须是整数"
            if options["cdc_batch_size"] < 1:
                return None, "高级选项 cdc_batch_size 必须 >= 1"
    return options, None


def _apply_model_ttl(get, mapping: list[dict], options: dict) -> str | None:
    """表模型（key/聚合校验）+ TTL 应用到 mapping/options；get 为表单取值 callable。

    返回错误信息或 None。批量建作业时 get 按对象前缀取值，实现逐对象模型/TTL。
    """
    # 目标表模型：仅非默认时存储（DUPLICATE 为默认，保持 options 干净）
    table_model = (get("table_model") or "").strip().upper()
    if table_model in ("UNIQUE", "AGGREGATE"):
        options["table_model"] = table_model
    elif table_model == "DUPLICATE":
        for m in mapping:
            m.pop("is_key", None)  # DUPLICATE 忽略行级 key 标记（UI 已隐藏，双保险）
    if table_model == "AGGREGATE":
        for m in mapping:
            if m.get("nested") or m["doris_type"] == "VARIANT":
                return f"AGGREGATE 模型不支持 nested/VARIANT 列: {m['doris_col']}"
            t = m["doris_type"]
            if (t == "STRING" or t.startswith("VARCHAR")) \
                    and m.get("agg", "REPLACE") != "REPLACE":
                return f"VARCHAR/STRING 列只允许 REPLACE 聚合: {m['doris_col']}"
    # TTL（动态分区留存）：数值 + 粒度单位；填了数值就必须选时间字段
    ttl_num_raw = (get("ttl_num") or get("ttl_days") or "").strip()
    if ttl_num_raw:
        try:
            ttl_num = int(ttl_num_raw)
        except ValueError:
            return "TTL 留存时长必须是整数"
        if ttl_num < 1:
            return "TTL 留存时长必须 >= 1"
        ttl_unit = (get("ttl_unit") or "DAY").strip().upper()
        if ttl_unit not in ("HOUR", "DAY", "WEEK", "MONTH"):
            return f"TTL 粒度非法: {ttl_unit}（Doris 动态分区仅支持 HOUR/DAY/WEEK/MONTH）"
        ttl_col = (get("ttl_column") or "").strip()
        if not ttl_col:
            return "设置了 TTL 留存时长，请选择 TTL 时间字段"
        col = next((m for m in mapping if m["doris_col"] == ttl_col), None)
        if col is None:
            return f"TTL 时间字段不在字段映射中: {ttl_col}"
        col_type = col["doris_type"]
        if col_type == "BIGINT":
            # epoch 整数列：以 DATETIMEV2(3) 存储，stream load 时 from_millisecond 转换
            # （毫秒/微秒/纳秒按数值量级在表达式里自适应，见 render._epoch_expr）
            col["ms_epoch"] = True
            col["doris_type"] = "DATETIMEV2(3)"
        elif col_type == "STRING":
            # 日期格式字符串列（'yyyy-MM-dd'）：以 DATE 存储，Doris 直接解析无需转换
            col["doris_type"] = "DATE"
        elif not col_type.startswith(("DATE", "DATETIME")):
            return f"TTL 时间字段必须是 DATE/DATETIME/BIGINT(epoch 毫秒)/STRING(日期字符串) 类型列: {ttl_col}"
        options["ttl_num"] = ttl_num
        options["ttl_unit"] = ttl_unit
        options["ttl_column"] = ttl_col
        # 预建历史分区数（可选）：全量/存量同步时，数据落在动态分区窗口（start~end）之外
        # 会整批失败（no partition for this tuple）；预建历史分区后先灌入、TTL 到期自清理
        history_raw = (get("ttl_history_num") or "").strip()
        if history_raw:
            try:
                history_num = int(history_raw)
            except ValueError:
                return "TTL 预建历史分区数必须是整数"
            if history_num < 1:
                return "TTL 预建历史分区数必须 >= 1"
            options["ttl_history_num"] = history_num
    return None


def _collect_options(form, mapping: list[dict]) -> tuple[dict | None, str | None]:
    """从表单收集高级选项（共享部分 + 模型/TTL）；返回 (options, 错误信息)。create/edit 共用。"""
    options, err = _shared_options(form)
    if err:
        return None, err
    err = _apply_model_ttl(form.get, mapping, options)
    if err:
        return None, err
    return options, None


def _auto_mapping(db: Session, env: str, source_type: str, ds: Datasource,
                  source_ref: str, pkg: ProtoPackage | None, message_name: str | None,
                  add_timestamps: bool) -> list[dict] | None:
    """兜底：表单未回传映射时按数据源元数据/proto 自动生成；元数据缺失/解析异常返回 None。"""
    from .json_api import _source_columns

    try:
        variant_enabled = bool(envs.get_env(db, env)["doris"].get("variant_enabled", True))
        if source_type == "kafka":
            columns = proto_center.schema_fields_for(pkg, message_name)
        else:
            columns = _source_columns(ds, source_ref)
    except Exception:  # noqa: BLE001 - proto 包 error 状态/parsed 为空等，走"元数据缺失"回显
        return None
    if not columns:
        return None
    mapping = build_mapping(source_type, columns, variant_enabled)
    if add_timestamps:
        append_timestamp_columns(mapping, source_type)
    return mapping


@router.post("/jobs")
async def job_create(request: Request, db: Session = Depends(get_db)):
    """保存作业（DRAFT）并尝试首次渲染 conf；校验失败回显表单。"""
    form = await request.form(max_fields=10000)

    def _err(msg: str):
        return templates.TemplateResponse(request, "job_form.html", {
            "active": "jobs", "env_names": envs.env_names(db),
            "ds_types": DS_TYPES, "error": msg, "form": _form_dict(form),
            "job": None, "mapping": [], "joptions": {}, "add_ts": True,
        }, status_code=400)

    name = (form.get("name") or "").strip()
    env = (form.get("env") or "").strip()
    tags = (form.get("tags") or "").strip()
    source_type = (form.get("source_type") or "").strip()
    source_ref = (form.get("source_ref") or "").strip()
    doris_db = (form.get("doris_db") or "").strip()
    doris_table = (form.get("doris_table") or "").strip()
    # 业务线概念并入目标 Doris 库（容量面板按它聚合），不再让用户填写
    biz_line = doris_db

    if not _NAME_RE.match(name):
        return _err("作业名称必填，仅限字母/数字/_.-")
    if env not in envs.env_names(db):
        return _err("请选择环境")
    if source_type not in DS_TYPES:
        return _err("请选择源类型")
    try:
        ds = db.get(Datasource, int(form.get("datasource_id") or 0))
    except (TypeError, ValueError):
        ds = None
    if not ds or ds.env != env or ds.type != source_type:
        return _err("数据源无效（需与环境、源类型匹配）")
    if not source_ref:
        return _err("请选择源对象")
    pkg = None
    message_name = None
    if source_type == "kafka":
        try:
            pkg = db.get(ProtoPackage, int(form.get("proto_package_id") or 0))
        except (TypeError, ValueError):
            pkg = None
        message_name = (form.get("message_name") or "").strip()
        if not pkg or not message_name:
            return _err("kafka 作业必须选择 proto 包与 message")
    if not _IDENT_RE.match(doris_db):
        return _err("Doris 库名非法（字母/数字/下划线，不能以数字开头）")
    if not _IDENT_RE.match(doris_table):
        return _err("Doris 表名非法（字母/数字/下划线，不能以数字开头）")

    # 字段映射（预览表格回传，Doris 列名/类型允许用户改过）
    add_timestamps = (form.get("add_timestamps") or "").strip() == "on"
    mapping, merr = _parse_mapping_form(form)
    if merr:
        return _err(merr)
    if mapping is None:
        # 兜底：表单未回传映射（如跳过预览直接提交），服务端按同样规则自动生成
        mapping = _auto_mapping(db, env, source_type, ds, source_ref, pkg, message_name,
                                add_timestamps)
        if mapping is None:
            return _err("无法自动生成字段映射（元数据缺失），请先在数据源详情页刷新元数据")

    # 高级选项（含 TTL）
    options, oerr = _collect_options(form, mapping)
    if oerr:
        return _err(oerr)

    job = Job(
        name=name, env=env, biz_line=biz_line, tags=tags,
        source_type=source_type, datasource_id=ds.id, source_ref=source_ref,
        doris_db=doris_db, doris_table=doris_table,
        proto_package_id=pkg.id if pkg else None,
        message_name=message_name,
        field_mapping_json=Job._dumps(mapping),
        options_json=Job._dumps(options),
        status="DRAFT",
    )
    job.datasource = ds
    job.proto_package = pkg
    db.add(job)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名作业: {name}")

    # 首次渲染 conf 留档（失败不阻断，提交时会重渲染）
    warn = None
    try:
        render.render_and_save(db, job, note="create")
    except Exception as e:  # noqa: BLE001
        warn = f"conf 预渲染失败（提交时会重试）: {e}"
    return goto(request, f"/jobs/{job.id}", warn or f"作业 {name} 已创建（DRAFT）",
                ok=warn is None)


# ---------------------------------------------------------------- 批量建作业

_BATCH_MAX = 200


def _batch_defaults(source_type: str, source_ref: str) -> tuple[str, str]:
    """默认命名：kafka 作业名 = topic 点分末段大写，其余 = 源表名；目标表沿用 {类型}_{末段小写}。"""
    last = source_ref.split(".")[-1]
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in last)
    name = safe.upper() if source_type == "kafka" else safe
    return name, f"{source_type}_{safe.lower()}"


def _batch_shared_ctx(form) -> dict:
    """批量共享配置（step1 校验与 step2 隐藏域透传共用）；键名即表单字段名。"""
    keys = ("env", "source_type", "datasource_id", "proto_package_id", "message_name",
            "tags", "doris_db", "add_timestamps", "parallelism", "checkpoint_interval",
            "buckets", "start_mode", "mongo_mode", "cdc_startup_mode", "cdc_batch_size")
    return {k: (form.get(k) or "").strip() for k in keys}


def _batch_validate_shared(db: Session, ctx: dict):
    """校验共享配置；返回 (ds, pkg, 错误信息)。"""
    if ctx["env"] not in envs.env_names(db):
        return None, None, "请选择环境"
    if ctx["source_type"] not in DS_TYPES:
        return None, None, "请选择源类型"
    try:
        ds = db.get(Datasource, int(ctx["datasource_id"] or 0))
    except (TypeError, ValueError):
        ds = None
    if not ds or ds.env != ctx["env"] or ds.type != ctx["source_type"]:
        return None, None, "数据源无效（需与环境、源类型匹配）"
    pkg = None
    if ctx["source_type"] == "kafka":
        try:
            pkg = db.get(ProtoPackage, int(ctx["proto_package_id"] or 0))
        except (TypeError, ValueError):
            pkg = None
        if not pkg or not ctx["message_name"]:
            return None, None, "kafka 批量作业必须选择 proto 包与 message"
    if not _IDENT_RE.match(ctx["doris_db"]):
        return None, None, "Doris 库名非法（字母/数字/下划线，不能以数字开头）"
    return ds, pkg, None


def _batch_mapping_for(db: Session, ds: Datasource, pkg: ProtoPackage | None,
                       ctx: dict, source_ref: str, flatten=frozenset()) -> list[dict] | None:
    """为单个源对象生成字段映射（kafka 走 proto，其余走元数据缓存）；无字段返回 None。"""
    from .json_api import _source_columns

    variant_enabled = bool(envs.get_env(db, ctx["env"])["doris"].get("variant_enabled", True))
    if ctx["source_type"] == "kafka":
        columns = proto_center.flattened_schema_fields(pkg, ctx["message_name"], flatten)
    else:
        columns = _source_columns(ds, source_ref)
    if not columns:
        return None
    mapping = build_mapping(ctx["source_type"], columns, variant_enabled)
    if ctx["add_timestamps"] == "on":
        append_timestamp_columns(mapping, ctx["source_type"])
    return mapping


@router.get("/jobs/batch-new", response_class=HTMLResponse)
def job_batch_new(request: Request, db: Session = Depends(get_db)):
    """批量建作业 step1：共享配置 + 数据源 + 源对象多选。"""
    return templates.TemplateResponse(request, "job_batch_form.html", {
        "active": "jobs", "env_names": envs.env_names(db),
        "ds_types": DS_TYPES, "error": None, "form": {},
    })


@router.post("/jobs/batch-new/preview", response_class=HTMLResponse)
async def job_batch_preview(request: Request, db: Session = Depends(get_db)):
    """批量建作业 step2：逐对象配置（默认命名 + 自动映射，可逐个展开编辑）。"""
    form = await request.form(max_fields=10000)
    ctx = _batch_shared_ctx(form)

    def _err(msg: str):
        return templates.TemplateResponse(request, "job_batch_form.html", {
            "active": "jobs", "env_names": envs.env_names(db),
            "ds_types": DS_TYPES, "error": msg, "form": _form_dict(form),
        }, status_code=400)

    ds, pkg, err = _batch_validate_shared(db, ctx)
    if err:
        return _err(err)
    objects = [o.strip() for o in form.getlist("objects") if o.strip()]
    if not objects:
        return _err("请至少勾选一个源对象")
    if len(objects) > _BATCH_MAX:
        return _err(f"单次批量最多 {_BATCH_MAX} 个源对象，请分批创建")

    items = []
    for i, ref in enumerate(objects):
        name, table = _batch_defaults(ctx["source_type"], ref)
        item = {"i": i, "p": f"o{i}_", "ref": ref, "name": name,
                "doris_table": table, "mapping": [], "error": None}
        try:
            mapping = _batch_mapping_for(db, ds, pkg, ctx, ref)
            if mapping is None:
                item["error"] = "元数据/proto 中找不到该对象的字段，无法生成映射"
            else:
                item["mapping"] = mapping
        except Exception as e:  # noqa: BLE001 - 单个对象失败不拖垮整批预览
            item["error"] = f"生成映射失败: {e}"
        items.append(item)
    return templates.TemplateResponse(request, "job_batch_preview.html", {
        "active": "jobs", "ctx": ctx, "ds": ds, "pkg": pkg, "items": items,
    })


@router.post("/jobs/batch-create", response_class=HTMLResponse)
async def job_batch_create(request: Request, db: Session = Depends(get_db)):
    """批量创建 DRAFT 作业：逐对象独立校验/落库，单个失败不影响其余，结果页汇总。"""
    form = await request.form(max_fields=10000)
    ctx = _batch_shared_ctx(form)
    ds, pkg, serr = _batch_validate_shared(db, ctx)
    shared, oerr = _shared_options(form)
    if serr or oerr:
        return goto(request, "/jobs/batch-new", f"批量创建失败: {serr or oerr}", ok=False)

    results: list[dict] = []
    seen_names: set[str] = set()
    for raw_i in form.getlist("o_idx"):
        p = f"o{raw_i}_"
        ref = (form.get(f"{p}ref") or "").strip()
        name = (form.get(f"{p}name") or "").strip()
        table = (form.get(f"{p}doris_table") or "").strip()
        if not ref:
            continue

        def _fail(reason: str, _ref=ref, _name=name) -> None:
            results.append({"ref": _ref, "name": _name, "ok": False,
                            "error": reason, "job": None})

        if not _NAME_RE.match(name):
            _fail("作业名非法（仅限字母/数字/_.-）")
            continue
        if not _IDENT_RE.match(table):
            _fail("目标表名非法（字母/数字/下划线，不能以数字开头）")
            continue
        if name in seen_names:
            _fail("本批次内作业名重复")
            continue
        seen_names.add(name)
        if db.query(Job).filter(Job.name == name).count():
            _fail("已存在同名作业")
            continue
        dup = db.query(Job).filter(Job.datasource_id == ds.id,
                                   Job.source_ref == ref).first()
        if dup:
            _fail(f"已有作业 {dup.name} 使用同一源对象（如需重建请先删除该作业）")
            continue
        mapping, merr = _parse_mapping_form(form, p)
        if merr:
            _fail(merr)
            continue
        if mapping is None:
            _fail("字段映射缺失（预览页该对象映射未生成）")
            continue
        options = dict(shared)
        terr = _apply_model_ttl(lambda k, _p=p: form.get(f"{_p}{k}"), mapping, options)
        if terr:
            _fail(terr)
            continue

        job = Job(
            name=name, env=ctx["env"], biz_line=ctx["doris_db"], tags=ctx["tags"],
            source_type=ctx["source_type"], datasource_id=ds.id, source_ref=ref,
            doris_db=ctx["doris_db"], doris_table=table,
            proto_package_id=pkg.id if pkg else None,
            message_name=ctx["message_name"] or None,
            field_mapping_json=Job._dumps(mapping),
            options_json=Job._dumps(options),
            status="DRAFT",
        )
        job.datasource = ds
        job.proto_package = pkg
        db.add(job)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            _fail("作业名冲突（并发创建）")
            continue
        warn = None
        try:
            render.render_and_save(db, job, note="batch create")
        except Exception as e:  # noqa: BLE001
            warn = f"conf 预渲染失败（提交时会重试）: {e}"
        results.append({"ref": ref, "name": name, "ok": True, "error": warn, "job": job})

    ok_cnt = sum(1 for r in results if r["ok"])
    return templates.TemplateResponse(request, "job_batch_result.html", {
        "active": "jobs", "results": results, "ok_cnt": ok_cnt,
    })


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int, db: Session = Depends(get_db)):
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    return templates.TemplateResponse(request, "job_detail.html", {
        "active": "jobs", "job": job,
        "env_names": envs.env_names(db),
    })


# ---------------------------------------------------------------- 作业编辑

def _job_form_ctx(db: Session, job: Job, form: dict, error: str | None) -> dict:
    """编辑模式 job_form 的模板上下文（GET 与校验失败回显共用）。"""
    mapping = job.field_mapping
    return {
        "active": "jobs", "env_names": envs.env_names(db),
        "ds_types": DS_TYPES, "error": error, "form": form,
        "job": job, "mapping": mapping, "joptions": job.options,
        "add_ts": any(m.get("sink_only") for m in mapping),
    }


@router.get("/jobs/{job_id}/edit", response_class=HTMLResponse)
def job_edit(request: Request, job_id: int, db: Session = Depends(get_db)):
    """作业编辑页：名称/环境/源只读，可改标签、目标库表、字段映射、TTL、高级选项。"""
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    return templates.TemplateResponse(request, "job_form.html",
                                      _job_form_ctx(db, job, {}, None))


@router.post("/jobs/{job_id}/edit")
async def job_update(request: Request, job_id: int, db: Session = Depends(get_db)):
    """保存作业配置（不自动重启）：更新标签/目标库表/映射/选项并重渲染 conf。"""
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    form = await request.form(max_fields=10000)

    def _err(msg: str):
        return templates.TemplateResponse(request, "job_form.html",
                                          _job_form_ctx(db, job, _form_dict(form), msg),
                                          status_code=400)

    tags = (form.get("tags") or "").strip()
    doris_db = (form.get("doris_db") or "").strip()
    doris_table = (form.get("doris_table") or "").strip()
    if not _IDENT_RE.match(doris_db):
        return _err("Doris 库名非法（字母/数字/下划线，不能以数字开头）")
    if not _IDENT_RE.match(doris_table):
        return _err("Doris 表名非法（字母/数字/下划线，不能以数字开头）")

    # 字段映射：优先表单回传；未回传时按现有数据源/proto 重新生成
    mapping, merr = _parse_mapping_form(form)
    if merr:
        return _err(merr)
    if mapping is None:
        add_timestamps = (form.get("add_timestamps") or "").strip() == "on"
        mapping = _auto_mapping(db, job.env, job.source_type, job.datasource,
                                job.source_ref, job.proto_package, job.message_name,
                                add_timestamps)
        if mapping is None:
            return _err("无法重新生成字段映射（元数据缺失），请先在数据源详情页刷新元数据")

    options, oerr = _collect_options(form, mapping)
    if oerr:
        return _err(oerr)

    job.tags = tags
    job.doris_db = doris_db
    job.doris_table = doris_table
    job.biz_line = doris_db  # 业务线概念并入目标 Doris 库
    job.field_mapping_json = Job._dumps(mapping)
    job.options_json = Job._dumps(options)
    db.add(job)
    db.commit()

    # 重渲染 conf 留档（失败不阻断，更新并重启时会重渲染）
    warn = None
    try:
        render.render_and_save(db, job, note="edit")
    except Exception as e:  # noqa: BLE001
        warn = f"配置已保存，但 conf 重渲染失败（更新时会重试）: {e}"
    return goto(request, f"/jobs/{job_id}",
                warn or "配置已保存，点击「更新并重启」使新配置生效", ok=warn is None)


def _back_url(request: Request, job_id: int) -> str:
    """操作完成后回到发起页（列表页操作回列表含筛选参数，详情页操作回详情）。

    htmx 会带 HX-Current-URL 头（浏览器当前地址）；非 htmx 回退到作业详情。
    """
    cur = request.headers.get("HX-Current-URL", "")
    path = urlparse(cur).path if cur else ""
    if not path or not path.startswith("/"):
        return f"/jobs/{job_id}"
    query = urlparse(cur).query
    return path + (f"?{query}" if query else "")


def _do_orchestrate(request: Request, db: Session, job_id: int, action: str):
    """作业编排操作公共入口：取作业 -> 调 orchestrator -> flash 跳转回发起页。"""
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    back = _back_url(request, job_id)
    if action == "submit":
        result = orchestrator.submit(db, job)
    elif action == "stop":
        result = orchestrator.stop(db, job)
    elif action == "restart":
        result = orchestrator.submit(db, job, start_with_savepoint=True)
    elif action == "update":
        result = orchestrator.update_and_restart(db, job, note="manual update")
    else:  # pragma: no cover
        raise ValueError(action)
    if result.get("ok"):
        return goto(request, back, f"操作成功: {action}")
    return goto(request, back,
                f"操作失败: {result.get('error', '未知错误')}", ok=False)


@router.post("/jobs/{job_id}/submit")
def job_submit(request: Request, job_id: int, db: Session = Depends(get_db)):
    return _do_orchestrate(request, db, job_id, "submit")


@router.post("/jobs/{job_id}/stop")
def job_stop(request: Request, job_id: int, db: Session = Depends(get_db)):
    return _do_orchestrate(request, db, job_id, "stop")


@router.post("/jobs/{job_id}/restart")
def job_restart(request: Request, job_id: int, db: Session = Depends(get_db)):
    return _do_orchestrate(request, db, job_id, "restart")


@router.post("/jobs/{job_id}/update")
def job_update(request: Request, job_id: int, db: Session = Depends(get_db)):
    return _do_orchestrate(request, db, job_id, "update")


@router.post("/jobs/{job_id}/refresh-status", response_class=HTMLResponse)
def job_refresh_status(request: Request, job_id: int, db: Session = Depends(get_db)):
    """同步 SeaTunnel 侧状态，返回徽章片段（局部刷新）。"""
    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<span class="badge badge-error">作业不存在</span>')
    orchestrator.refresh_status(db, job)
    return templates.TemplateResponse(request, "_badge_poller.html", {"job": job})


@router.get("/jobs/{job_id}/badge", response_class=HTMLResponse)
def job_badge(request: Request, job_id: int, db: Session = Depends(get_db)):
    """状态徽章轮询片段（详情页 every 15s 自动刷新）。"""
    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<span class="badge badge-error">作业不存在</span>')
    return templates.TemplateResponse(request, "_badge_poller.html", {"job": job})


@router.post("/jobs/{job_id}/copy")
async def job_copy(request: Request, job_id: int, db: Session = Depends(get_db)):
    """复制作业：复制全部配置到目标环境（可同环境）的同类数据源，DRAFT，跳编辑页。"""
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    form = await request.form()
    target_env = (form.get("target_env") or "").strip()
    new_name = (form.get("new_name") or "").strip() or f"{job.name}_copy"
    if target_env not in envs.env_names(db):
        return goto(request, f"/jobs/{job_id}", "目标环境非法", ok=False)
    if not _NAME_RE.match(new_name):
        return goto(request, f"/jobs/{job_id}", "新作业名称非法", ok=False)
    try:
        target_ds = db.get(Datasource, int(form.get("target_datasource_id") or 0))
    except (TypeError, ValueError):
        target_ds = None
    if not target_ds or target_ds.env != target_env or target_ds.type != job.source_type:
        return goto(request, f"/jobs/{job_id}",
                    "目标数据源无效（需位于目标环境且类型一致）", ok=False)

    new_job = Job(
        name=new_name, env=target_env, biz_line=job.biz_line, tags=job.tags,
        source_type=job.source_type, datasource_id=target_ds.id,
        source_ref=job.source_ref, doris_db=job.doris_db, doris_table=job.doris_table,
        proto_package_id=job.proto_package_id, message_name=job.message_name,
        field_mapping_json=job.field_mapping_json, options_json=job.options_json,
        status="DRAFT",
    )
    new_job.datasource = target_ds
    new_job.proto_package = job.proto_package
    db.add(new_job)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return goto(request, f"/jobs/{job_id}", f"已存在同名作业: {new_name}", ok=False)
    try:
        render.render_and_save(db, new_job, note=f"copy from {job.name}")
    except Exception:  # noqa: BLE001 - 编辑页保存时会重渲染，这里失败不阻断
        pass
    return goto(request, f"/jobs/{new_job.id}/edit",
                f"已复制作业为 {new_name}（DRAFT），请检查配置后保存")


@router.delete("/jobs/{job_id}")
def job_delete(request: Request, job_id: int, db: Session = Depends(get_db)):
    """删除作业 = 取消 SeaTunnel 侧作业（如在跑）+ 删除平台记录。

    SeaTunnel 没有"删除作业"概念：作业只能 cancel 到终态（FINISHED/CANCELED），
    终态后只是 master 里的历史记录，不占资源。因此删除流程：
    在跑 → /stop-job（不带 savepoint）取消；已终态 → 直接删记录。
    """
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    note = ""
    if job.seatunnel_job_id:
        from ..services.orchestrator import _get, _post

        try:
            # 连接级异常会抛出（拒绝删除）；查不到作业按已终态处理
            info = _get(db, job.env, f"/job-info/{job.seatunnel_job_id}")
            if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
                _post(db, job.env, "/stop-job",
                      json={"jobId": int(job.seatunnel_job_id), "isStopWithSavePoint": False})
                note = f"，SeaTunnel 侧运行中作业已取消（jobId={job.seatunnel_job_id}）"
            else:
                note = "，SeaTunnel 侧已是终态（仅历史记录，无需处理）" if info else ""
        except Exception as e:  # noqa: BLE001
            return goto(request, f"/jobs/{job_id}",
                        f"无法确认/取消 SeaTunnel 侧作业（{e}），未删除。请确认集群可达后重试",
                        ok=False)
    # 级联清理指标样本（versions/events 由 ORM cascade 处理，MetricSample 不在 relationship 里）
    db.query(MetricSample).filter(MetricSample.job_id == job.id).delete()
    db.delete(job)
    db.commit()
    return goto(request, "/jobs", f"作业 {job.name} 已删除{note}")


# ---------------------------------------------------------------- 监控面板

@router.get("/monitor", response_class=HTMLResponse)
def monitor_page(request: Request, env: str = "", db: Session = Depends(get_db)):
    """环境级监控：SeaTunnel/Doris 集群状态 + 作业汇总 + 速率趋势图。"""
    from ..services import monitor as mon

    names = envs.env_names(db)
    cur = env if env in names else (names[0] if names else "")
    st = mon.seatunnel_cluster(db, cur) if cur else None
    dr = mon.doris_cluster(db, cur) if cur else None
    jobs = db.query(Job).filter(Job.env == cur).all() if cur else []
    job_stats = {
        "total": len(jobs),
        "running": sum(1 for j in jobs if j.status == "RUNNING"),
        "failed": sum(1 for j in jobs if j.status in ("FAILED", "ERROR")),
        "stopped": sum(1 for j in jobs if j.status == "STOPPED"),
    }
    return templates.TemplateResponse(request, "capacity.html", {
        "active": "monitor", "env_names": names, "cur_env": cur,
        "st": st, "dr": dr, "job_stats": job_stats,
    })


@router.get("/capacity", response_class=HTMLResponse)
def capacity_redirect(request: Request):
    """老路由重定向到 /monitor。"""
    return RedirectResponse("/monitor", status_code=302)


@router.get("/jobs/{job_id}/monitor", response_class=HTMLResponse)
def job_monitor_panel(request: Request, job_id: int, db: Session = Depends(get_db)):
    """作业监控指标块片段（详情页 load 时异步加载，失败降级为错误条）。"""
    from ..services import monitor as mon

    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<div class="alert alert-error">作业不存在</div>')
    return templates.TemplateResponse(request, "_job_monitor.html", {
        "job": job,
        "ws": mon.job_write_stats(db, job),
        "cp": mon.checkpoint_stats(db, job),
        "lag": mon.kafka_lag(db, job),
        "ts": mon.doris_table_stats(db, job),
    })


@router.get("/jobs/{job_id}/doris-rows", response_class=HTMLResponse)
def job_doris_rows(request: Request, job_id: int, db: Session = Depends(get_db)):
    """按需 COUNT(*) 查目标表行数（大表昂贵，按钮触发）。"""
    from html import escape

    from ..services import monitor as mon

    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<span class="test-fail">作业不存在</span>')
    res = mon.doris_table_rows(db, job)
    if res["error"]:
        return HTMLResponse(f'<span class="test-fail">查询失败: {escape(res["error"])}</span>')
    return HTMLResponse(f'<span class="mono">{res["rows"]:,} 行</span>')


def _recreate_ctx(db: Session, job: Job) -> dict:
    """重建页上下文：兼容性判定 + 迁移计划（Doris 不可达时 error 降级）。"""
    from ..services import doris_ddl
    from ..services.orchestrator import _job_buckets, _job_model, _job_ttl

    ctx: dict = {"compat": None, "plan": [], "dropped": [], "error": None}
    try:
        compat = doris_ddl.check_compat(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table,
            job.field_mapping, _job_ttl(job), _job_model(job), _job_buckets(job))
        ctx["compat"] = compat
        if compat["exists"]:
            variant = bool(envs.get_env(db, job.env)["doris"].get("variant_enabled", True))
            desired_keys = doris_ddl._key_columns(
                job.field_mapping,
                {"column": _job_ttl(job)["column"]} if _job_ttl(job) else None)
            plan, dropped = doris_ddl.build_migration_plan(
                compat["old_cols"], job.field_mapping, variant, desired_keys)
            ctx["plan"] = plan
            ctx["dropped"] = dropped
    except Exception as e:  # noqa: BLE001 - Doris 不可达：页面降级，仍允许删表重建（会在执行时报错）
        ctx["error"] = f"无法读取目标表现状（{e}）"
    return ctx


@router.get("/jobs/{job_id}/recreate-table", response_class=HTMLResponse)
def job_recreate_page(request: Request, job_id: int, db: Session = Depends(get_db)):
    """重建目标表选择页：现状 vs 新配置对比，删表重建 / 数据迁移重建 两个选项。"""
    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    return templates.TemplateResponse(request, "job_recreate.html", {
        "active": "jobs", "job": job, "errors": [],
        **_recreate_ctx(db, job),
    })


@router.post("/jobs/{job_id}/recreate-table")
async def job_recreate_table(request: Request, job_id: int, db: Session = Depends(get_db)):
    """执行重建：mode=drop 删表重建（数据丢失）；mode=migrate 数据迁移重建。

    migrate 编排：RUNNING 先 stop(savepoint) -> RENAME 为 tmp_ -> 建新表 -> INSERT SELECT
    -> 行数核对 -> 删 tmp -> 按新配置 savepoint 恢复（kafka 位点保留，过程不丢数据）；
    迁移失败自动回滚表名并尽量把作业拉回原运行状态。
    """
    from ..services import doris_ddl
    from ..services.orchestrator import _job_buckets, _job_model, _job_ttl, _wait_terminal

    job = db.get(Job, job_id)
    if not job:
        return goto(request, "/jobs", "作业不存在", ok=False)
    form = await request.form()
    mode = (form.get("mode") or "").strip()
    if mode not in ("drop", "migrate"):
        return goto(request, f"/jobs/{job.id}", f"非法的重建方式: {mode or '(空)'}", ok=False)
    doris = envs.get_env(db, job.env)["doris"]

    if mode == "drop":
        if job.status in ("RUNNING", "UPDATING"):
            return goto(request, f"/jobs/{job.id}",
                        "作业运行中/更新中，请先停止再删表重建（或选数据迁移，会自动编排停启）", ok=False)
        try:
            res = doris_ddl.recreate_table(
                doris, job.doris_db, job.doris_table,
                job.field_mapping, _job_ttl(job), _job_model(job), _job_buckets(job))
            db.add(JobEvent(job_id=job.id, event="ddl",
                            detail=f"删表重建 {job.doris_db}.{job.doris_table}:\n{res['ddl']}"))
            db.commit()
            return goto(request, f"/jobs/{job.id}",
                        f"目标表 {job.doris_db}.{job.doris_table} 已按当前配置重建（原数据已清空）")
        except Exception as e:  # noqa: BLE001
            return goto(request, f"/jobs/{job.id}", f"删表重建失败: {e}", ok=False)

    # ---- mode == "migrate" ----
    was_running = job.status == "RUNNING"
    if job.status == "UPDATING":
        return goto(request, f"/jobs/{job.id}", "作业正在更新编排中，稍后再试", ok=False)

    compat = doris_ddl.check_compat(doris, job.doris_db, job.doris_table, job.field_mapping,
                                    _job_ttl(job), _job_model(job), _job_buckets(job))
    if not compat["exists"]:
        return goto(request, f"/jobs/{job.id}",
                    "目标表不存在，无需迁移（直接提交作业即可自动建表）", ok=False)
    variant = bool(doris.get("variant_enabled", True))
    desired_keys = doris_ddl._key_columns(
        job.field_mapping, {"column": _job_ttl(job)["column"]} if _job_ttl(job) else None)
    plan, _dropped = doris_ddl.build_migration_plan(
        compat["old_cols"], job.field_mapping, variant, desired_keys)
    decisions = {k: v for k, v in form.items() if isinstance(v, str)}
    exprs, errs = doris_ddl.build_select_exprs(plan, decisions)
    if errs:
        return templates.TemplateResponse(request, "job_recreate.html", {
            "active": "jobs", "job": job, "errors": errs,
            **_recreate_ctx(db, job),
        }, status_code=400)

    # 1) 运行中先带 savepoint 停止（kafka 位点保留，恢复后不漏数）
    old_st_id = job.seatunnel_job_id
    if was_running:
        stop_res = orchestrator.stop(db, job, with_savepoint=True)
        if not stop_res.get("ok"):
            return goto(request, f"/jobs/{job.id}",
                        f"迁移前停止作业失败，未动表: {stop_res.get('error')}", ok=False)
        if old_st_id:
            _wait_terminal(db, job.env, old_st_id)
        db.add(JobEvent(job_id=job.id, event="migrate", detail="已带 savepoint 停止，开始数据迁移"))
        db.commit()

    def _resume() -> str:
        """迁移结束后恢复作业（仅原本在运行的；原本 STOPPED 的保持停止，由用户手动启动）。

        失败不影响表结果，只提示。
        """
        if not was_running:
            return ""
        r = orchestrator.submit(db, job, start_with_savepoint=bool(old_st_id))
        return "" if r.get("ok") else f"；恢复作业失败（表已就绪，请手动提交）: {r.get('error')}"

    # 2) 迁移（内部失败自动回滚表名）
    try:
        mig = doris_ddl.migrate_table(
            doris, job.doris_db, job.doris_table, job.field_mapping,
            _job_ttl(job), _job_model(job), _job_buckets(job), exprs)
    except Exception as e:  # noqa: BLE001
        db.add(JobEvent(job_id=job.id, event="migrate", detail=f"迁移失败，已回滚为原表: {e}"))
        db.commit()
        resume_note = _resume()  # 表已回滚为原表，尽量把作业拉回原状态
        return goto(request, f"/jobs/{job.id}",
                    f"数据迁移失败（已回滚为原表，数据未丢失）: {e}{resume_note}", ok=False)

    db.add(JobEvent(
        job_id=job.id, event="migrate",
        detail=(f"数据迁移完成 {job.doris_db}.{job.doris_table}: 旧表 {mig['old_rows']} 行 -> "
                f"新表 {mig['new_rows']} 行"
                + (f"（补建历史分区 {len(mig['partitions_added'])} 个）"
                   if mig.get("partitions_added") else "")
                + ("（tmp 表已删除）" if mig["tmp_dropped"]
                   else f"（行数不一致，tmp 表 {mig['tmp']} 保留待人工核对）"))))
    db.commit()
    resume_note = _resume()
    note = "" if mig["tmp_dropped"] else f"；注意：tmp 表 {mig['tmp']} 未删除，请人工核对后清理"
    return goto(request, f"/jobs/{job.id}",
                f"数据迁移完成：{mig['old_rows']} -> {mig['new_rows']} 行{note}{resume_note}")
