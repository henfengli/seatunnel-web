"""数据源管理：CRUD + 连接测试 + 元数据刷新。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ...core.crypto import decrypt_conn, encrypt, mask_conn
from ...core.db import get_db
from ...models import DS_TYPES, Datasource, Job
from ...services import envs, health
from ...services.metadata import base as metadata
from ...templating import goto, templates
from .common import _NAME_RE, _form_dict, form_error

router = APIRouter()


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
        return form_error(request, "datasource_form.html", msg,
                          active="datasources", ds=None, conn=_form_dict(form),
                          env_names=envs.env_names(db), ds_types=DS_TYPES)

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
                    connection=conn)
    db.add(ds)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"环境 {env} 下已存在同名数据源: {name}")
    # 保存后自动测一次（失败不阻塞）+ 立即刷新元数据：真实连接探测，进线程池不卡事件循环
    await run_in_threadpool(health.check_datasource, db, ds)
    await run_in_threadpool(metadata.refresh, db, ds)  # 之后由 watchdog 每日刷新
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
        return templates.TemplateResponse(request, "_test_result.html", {
            "results": [(False, f"未知数据源类型: {ds_type or '(空)'}", "")],
        })
    try:
        old = db.get(Datasource, int(form.get("ds_id") or 0))
    except (TypeError, ValueError):
        old = None
    conn, err = _build_connection(ds_type, form)
    if err:
        return templates.TemplateResponse(request, "_test_result.html", {"results": [(False, err, "")]})
    if old is not None:  # 编辑时密码留空 = 用已存密码
        conn.setdefault("password", old.connection.get("password", ""))
        conn.setdefault("sasl_password", old.connection.get("sasl_password", ""))
    ok, msg = await run_in_threadpool(health.test_datasource, ds_type, decrypt_conn(conn))
    return templates.TemplateResponse(request, "_test_result.html", {"results": [(ok, msg, "")]})


@router.get("/datasources/{ds_id}", response_class=HTMLResponse)
def datasource_detail(request: Request, ds_id: int, db: Session = Depends(get_db)):
    ds = db.get(Datasource, ds_id)
    if not ds:
        return goto(request, "/datasources", "数据源不存在", ok=False)
    conn_display = mask_conn(ds.connection)  # 密码掩码展示，绝不明文回传
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
        return form_error(request, "datasource_form.html", msg,
                          active="datasources", ds=ds, conn={**ds.connection, **_form_dict(form)},
                          env_names=envs.env_names(db), ds_types=DS_TYPES)

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
    ds.connection = conn
    db.add(ds)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"环境 {env} 下已存在同名数据源: {name}")
    # 保存后自动测一次（失败不阻塞）+ 连接信息可能变了，立即自动刷新元数据
    await run_in_threadpool(health.check_datasource, db, ds)
    await run_in_threadpool(metadata.refresh, db, ds)
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

