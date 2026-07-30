"""作业管理：列表/新建/详情/编辑/提交编排/复制/删除/监控面板/目标表重建。"""
from __future__ import annotations

import re
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ...core.db import get_db
from ...models import DS_TYPES, JOB_STATUSES, Datasource, Job, JobEvent, MetricSample, ProtoPackage
from ...services import (doris_ddl, envs, monitor, orchestrator, proto_center, render,
                         seatunnel_client as st)
from ...services.field_mapping import append_timestamp_columns, build_mapping
from ...templating import goto, templates
from .common import _NAME_RE, _form_dict, form_error

router = APIRouter()

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
# Doris 类型白名单（防 map_doris_type 注入 DDL）：INT/VARCHAR(10)/DECIMAL(38,10)/DATETIMEV2(3) 等
_DORIS_TYPE_RE = re.compile(r"^[A-Z]+(V2)?(\(\d+(,\s*\d+)?\))?$")
# 列默认值白名单：CURRENT_TIMESTAMP[(3)] / 纯数字 / 单引号字符串字面量（内部单引号成双）
_DEFAULT_RE = re.compile(r"^(CURRENT_TIMESTAMP(\(3\))?|\d+|'([^']|'')*')$")


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
    from ..fragments import _source_columns

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
        return form_error(request, "job_form.html", msg,
                          active="jobs", env_names=envs.env_names(db), ds_types=DS_TYPES,
                          form=_form_dict(form), job=None, mapping=[], joptions={}, add_ts=True)

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
        return _err(f"已存在同名作业: {name}")

    # 首次渲染 conf 留档（失败不阻断，提交时会重渲染）
    warn = None
    try:
        await run_in_threadpool(render.render_and_save, db, job, note="create")
    except Exception as e:  # noqa: BLE001
        warn = f"conf 预渲染失败（提交时会重试）: {e}"
    return goto(request, f"/jobs/{job.id}", warn or f"作业 {name} 已创建（DRAFT）",
                ok=warn is None)


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
async def job_edit_save(request: Request, job_id: int, db: Session = Depends(get_db)):
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
    job.field_mapping = mapping
    job.options = options
    db.add(job)
    db.commit()

    # 重渲染 conf 留档（失败不阻断，更新并重启时会重渲染）
    warn = None
    try:
        await run_in_threadpool(render.render_and_save, db, job, note="edit")
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
def job_update_restart(request: Request, job_id: int, db: Session = Depends(get_db)):
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
        field_mapping=job.field_mapping, options=job.options,
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
        await run_in_threadpool(render.render_and_save, db, new_job, note=f"copy from {job.name}")
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
        try:
            # 连接级异常会抛出（拒绝删除）；查不到作业按已终态处理
            info = st.get(db, job.env, f"/job-info/{job.seatunnel_job_id}")
            if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
                st.post(db, job.env, "/stop-job",
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


@router.get("/jobs/{job_id}/monitor", response_class=HTMLResponse)
def job_monitor_panel(request: Request, job_id: int, db: Session = Depends(get_db)):
    """作业监控指标块片段（详情页 load 时异步加载，失败降级为错误条）。"""
    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<div class="alert alert-error">作业不存在</div>')
    return templates.TemplateResponse(request, "_job_monitor.html", {
        "job": job,
        "ws": monitor.job_write_stats(db, job),
        "cp": monitor.checkpoint_stats(db, job),
        "lag": monitor.kafka_lag(db, job),
        "ts": monitor.doris_table_stats(db, job),
    })


@router.get("/jobs/{job_id}/doris-rows", response_class=HTMLResponse)
def job_doris_rows(request: Request, job_id: int, db: Session = Depends(get_db)):
    """按需 COUNT(*) 查目标表行数（大表昂贵，按钮触发）。"""
    from html import escape

    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<span class="test-fail">作业不存在</span>')
    res = monitor.doris_table_rows(db, job)
    if res["error"]:
        return HTMLResponse(f'<span class="test-fail">查询失败: {escape(res["error"])}</span>')
    return HTMLResponse(f'<span class="mono">{res["rows"]:,} 行</span>')


def _recreate_ctx(db: Session, job: Job) -> dict:
    """重建页上下文：兼容性判定 + 迁移计划（Doris 不可达时 error 降级）。"""
    ctx: dict = {"compat": None, "plan": [], "dropped": [], "error": None}
    try:
        compat = doris_ddl.check_compat(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table,
            job.field_mapping, orchestrator.job_ttl(job), orchestrator.job_model(job), orchestrator.job_buckets(job))
        ctx["compat"] = compat
        if compat["exists"]:
            variant = bool(envs.get_env(db, job.env)["doris"].get("variant_enabled", True))
            desired_keys = doris_ddl.key_columns(
                job.field_mapping,
                {"column": orchestrator.job_ttl(job)["column"]} if orchestrator.job_ttl(job) else None)
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
            res = await run_in_threadpool(doris_ddl.recreate_table,
                doris, job.doris_db, job.doris_table,
                job.field_mapping, orchestrator.job_ttl(job), orchestrator.job_model(job), orchestrator.job_buckets(job))
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

    compat = await run_in_threadpool(
        doris_ddl.check_compat, doris, job.doris_db, job.doris_table, job.field_mapping,
        orchestrator.job_ttl(job), orchestrator.job_model(job), orchestrator.job_buckets(job))
    if not compat["exists"]:
        return goto(request, f"/jobs/{job.id}",
                    "目标表不存在，无需迁移（直接提交作业即可自动建表）", ok=False)
    variant = bool(doris.get("variant_enabled", True))
    desired_keys = doris_ddl.key_columns(
        job.field_mapping, {"column": orchestrator.job_ttl(job)["column"]} if orchestrator.job_ttl(job) else None)
    plan, _dropped = doris_ddl.build_migration_plan(
        compat["old_cols"], job.field_mapping, variant, desired_keys)
    decisions = {k: v for k, v in form.items() if isinstance(v, str)}
    exprs, errs = doris_ddl.build_select_exprs(plan, decisions)
    if errs:
        return templates.TemplateResponse(request, "job_recreate.html", {
            "active": "jobs", "job": job, "errors": errs,
            **(await run_in_threadpool(_recreate_ctx, db, job)),
        }, status_code=400)

    # 1) 运行中先带 savepoint 停止（kafka 位点保留，恢复后不漏数）
    old_st_id = job.seatunnel_job_id
    if was_running:
        stop_res = await run_in_threadpool(orchestrator.stop, db, job, with_savepoint=True)
        if not stop_res.get("ok"):
            return goto(request, f"/jobs/{job.id}",
                        f"迁移前停止作业失败，未动表: {stop_res.get('error')}", ok=False)
        if old_st_id:
            await run_in_threadpool(st.wait_terminal, db, job.env, old_st_id)
        db.add(JobEvent(job_id=job.id, event="migrate", detail="已带 savepoint 停止，开始数据迁移"))
        db.commit()

    async def _resume() -> str:
        """迁移结束后恢复作业（仅原本在运行的；原本 STOPPED 的保持停止，由用户手动启动）。

        失败不影响表结果，只提示。
        """
        if not was_running:
            return ""
        r = await run_in_threadpool(orchestrator.submit, db, job, start_with_savepoint=bool(old_st_id))
        return "" if r.get("ok") else f"；恢复作业失败（表已就绪，请手动提交）: {r.get('error')}"

    # 2) 迁移（内部失败自动回滚表名）
    try:
        mig = await run_in_threadpool(doris_ddl.migrate_table,
            doris, job.doris_db, job.doris_table, job.field_mapping,
            orchestrator.job_ttl(job), orchestrator.job_model(job), orchestrator.job_buckets(job), exprs)
    except Exception as e:  # noqa: BLE001
        db.add(JobEvent(job_id=job.id, event="migrate", detail=f"迁移失败，已回滚为原表: {e}"))
        db.commit()
        resume_note = await _resume()  # 表已回滚为原表，尽量把作业拉回原状态
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
    resume_note = await _resume()
    note = "" if mig["tmp_dropped"] else f"；注意：tmp 表 {mig['tmp']} 未删除，请人工核对后清理"
    return goto(request, f"/jobs/{job.id}",
                f"数据迁移完成：{mig['old_rows']} -> {mig['new_rows']} 行{note}{resume_note}")
