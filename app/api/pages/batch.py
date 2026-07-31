"""批量操作（启动/停止/重启/删除/改配置）+ 批量建作业向导。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ...core.db import get_db
from ...models import DS_TYPES, BatchTask, Datasource, Job, ProtoPackage
from ...services import batch_ops, envs, mapping_gen, render
from ...services.field_mapping import apply_model_ttl
from ...templating import goto, templates
from .common import (IDENT_RE, NAME_RE, form_dict, form_error,
                     parse_mapping_form, shared_options)

router = APIRouter()


@router.post("/jobs/batch")
async def job_batch_action(request: Request, db: Session = Depends(get_db)):
    """批量操作入口：勾选作业 + action -> 建 BatchTask 后台执行，跳进度页。"""
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
    """批量任务进度页。"""
    task = db.get(BatchTask, task_id)
    if not task:
        return goto(request, "/jobs", "批量任务不存在", ok=False)
    return templates.TemplateResponse(request, "batch_detail.html", {
        "active": "jobs", "task": task,
    })


@router.get("/batch/{task_id}/progress", response_class=HTMLResponse)
def batch_progress(request: Request, task_id: int, db: Session = Depends(get_db)):
    """进度块轮询片段（执行中每 2s 自刷新，DONE 后停止，与状态徽章同一套 htmx 轮询方式）。"""
    task = db.get(BatchTask, task_id)
    if not task:
        return HTMLResponse('<div class="alert alert-error">批量任务不存在</div>')
    return templates.TemplateResponse(request, "_batch_progress.html", {"task": task})



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
    if not IDENT_RE.match(ctx["doris_db"]):
        return None, None, "Doris 库名非法（字母/数字/下划线，不能以数字开头）"
    return ds, pkg, None


def _batch_mapping_for(db: Session, ds: Datasource, pkg: ProtoPackage | None,
                       ctx: dict, source_ref: str, flatten=frozenset()) -> list[dict] | None:
    """为单个源对象生成字段映射（统一走 services 层的 auto_mapping）；无字段返回 None。"""
    return mapping_gen.auto_mapping(db, ctx["env"], ctx["source_type"], ds, source_ref,
                                    pkg, ctx["message_name"],
                                    add_timestamps=ctx["add_timestamps"] == "on",
                                    flatten=flatten)


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
        return form_error(request, "job_batch_form.html", msg,
                          active="jobs", env_names=envs.env_names(db), ds_types=DS_TYPES,
                          form=form_dict(form))

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
    shared, oerr = shared_options(form)
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

        if not NAME_RE.match(name):
            _fail("作业名非法（仅限字母/数字/_.-）")
            continue
        if not IDENT_RE.match(table):
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
        mapping, merr = parse_mapping_form(form, p)
        if merr:
            _fail(merr)
            continue
        if mapping is None:
            _fail("字段映射缺失（预览页该对象映射未生成）")
            continue
        options = dict(shared)
        terr = apply_model_ttl(lambda k, _p=p: form.get(f"{_p}{k}"), mapping, options)
        if terr:
            _fail(terr)
            continue

        job = Job(
            name=name, env=ctx["env"], biz_line=ctx["doris_db"], tags=ctx["tags"],
            source_type=ctx["source_type"], datasource_id=ds.id, source_ref=ref,
            doris_db=ctx["doris_db"], doris_table=table,
            proto_package_id=pkg.id if pkg else None,
            message_name=ctx["message_name"] or None,
            field_mapping=mapping,
            options=options,
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
            await run_in_threadpool(render.render_and_save, db, job, note="batch create")
        except Exception as e:  # noqa: BLE001
            warn = f"conf 预渲染失败（提交时会重试）: {e}"
        results.append({"ref": ref, "name": name, "ok": True, "error": warn, "job": job})

    ok_cnt = sum(1 for r in results if r["ok"])
    return templates.TemplateResponse(request, "job_batch_result.html", {
        "active": "jobs", "results": results, "ok_cnt": ok_cnt,
    })

