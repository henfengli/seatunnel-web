"""环境管理：CRUD + 连接测试 + 引擎日志。"""
from __future__ import annotations

import re

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ...core.crypto import decrypt, encrypt
from ...core.db import get_db
from ...models import Datasource, Environment, Job
from ...services import envs, health, monitor
from ...templating import goto, templates
from .common import form_dict, form_error

router = APIRouter()


# ---------------------------------------------------------------- 环境

_ENVNAME_RE = re.compile(r"^[A-Za-z0-9_\-]{1,32}$")


def _env_form_apply(e: Environment, form) -> str | None:
    """校验表单并填充环境字段；密码/Auth 留空 = 不修改。返回错误信息或 None。"""
    name = (form.get("name") or "").strip()
    if not _ENVNAME_RE.match(name):
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
    return None


@router.get("/environments", response_class=HTMLResponse)
def environment_list(request: Request, db: Session = Depends(get_db)):
    counts = envs.job_status_counts(db)
    env_rows = []
    for env in envs.list_envs(db):
        status, title = health.env_aggregate(env.health)
        env_rows.append({
            "env": env,
            "masters": [m.split("://")[-1] for m in envs.parse_masters(env.seatunnel_masters)],
            "job_count": sum(counts.get(env.name, {}).values()),
            "health": status,
            "health_title": title,
            "parts": health.env_health_parts(env.health),
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
        return form_error(request, "environment_form.html", msg,
                          active="environments", env=None, form=form_dict(form), health=None)

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
    # 保存后自动测一次，失败不阻塞（真实连接探测，进线程池不卡事件循环）
    await run_in_threadpool(health.check_environment, db, env)
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
    res = await run_in_threadpool(health.test_environment, env_dict)
    return templates.TemplateResponse(request, "_test_result.html", {
        "results": [(*res["seatunnel"], "SeaTunnel"), (*res["doris"], "Doris")],
    })


@router.post("/environments/{env_id}/retest")
def environment_retest(request: Request, env_id: int, db: Session = Depends(get_db)):
    """重新测试已存环境并落库，跳回编辑页。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    health.check_environment(db, env)
    status, _ = health.env_aggregate(env.health)
    ok = status == "ok"
    return goto(request, f"/environments/{env_id}/edit",
                "连接测试通过" if ok else "连接测试未通过，详见健康状态", ok=ok)


@router.get("/environments/{env_id}/logs", response_class=HTMLResponse)
def environment_logs(request: Request, env_id: int, tail: int = 500,
                     db: Session = Depends(get_db)):
    """环境级引擎主日志页（SeaTunnel 主日志截尾；作业日志在作业详情页看）。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    return templates.TemplateResponse(request, "environment_logs.html", {
        "active": "environments", "env": env,
        "logs": monitor.engine_logs(db, env.name, min(max(tail, 50), 2000)),
    })


@router.get("/environments/{env_id}/edit", response_class=HTMLResponse)
def environment_edit(request: Request, env_id: int, db: Session = Depends(get_db)):
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    return templates.TemplateResponse(request, "environment_form.html", {
        "active": "environments", "env": env, "form": {},
        "health": health.env_aggregate(env.health), "error": None,
    })


@router.post("/environments/{env_id}")
async def environment_update(request: Request, env_id: int, db: Session = Depends(get_db)):
    """更新环境；密码/Auth 留空表示不修改。"""
    env = db.get(Environment, env_id)
    if not env:
        return goto(request, "/environments", "环境不存在", ok=False)
    form = await request.form()

    def _err(msg: str):
        return form_error(request, "environment_form.html", msg,
                          active="environments", env=env, form=form_dict(form),
                          health=health.env_aggregate(env.health))

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
    # 保存后自动测一次，失败不阻塞（真实连接探测，进线程池不卡事件循环）
    await run_in_threadpool(health.check_environment, db, env)
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

