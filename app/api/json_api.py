"""JSON/htmx 辅助路由：作业向导的联动下拉与字段映射预览（全部返回 HTML 片段）。

说明：除规范中的 /api/datasources/{id}/objects、/api/protos/{id}/messages 外，
另提供 query 参数版本（/api/datasources/objects、/api/protos/messages），
因为 htmx 的 hx-get URL 无法随下拉选中值动态变化，统一由它们代理到同一渲染逻辑。
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy.orm import Session

from ..core.config import get_settings
from ..core.db import get_db
from ..models import Datasource, Job, JobEvent, ProtoPackage
from ..services import doris_ddl, envs, monitor, proto_center
from ..services.field_mapping import append_timestamp_columns, build_mapping
from ..templating import templates

router = APIRouter(prefix="/api")


def _to_int(raw, default: int = 0) -> int:
    """查询参数容错转 int（htmx 可能把空字符串带上来）。"""
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------- 数据源下拉

@router.get("/datasources/options", response_class=HTMLResponse)
def datasource_options(request: Request, env: str = "", type: str = "",
                       source_type: str = "", target_env: str = "",
                       field_name: str = "datasource_id", with_objects: str = "1",
                       batch: str = "",
                       db: Session = Depends(get_db)):
    """按环境+类型返回数据源下拉框。

    target_env 为晋升表单的别名参数；field_name / with_objects 供晋升表单复用
    （select 改名、不联动源对象）。作业表单源类型字段名是 source_type，与 type 等价接受。
    batch=1 时选中联动到批量建作业的对象多选列表。
    """
    env = env or target_env
    type = type or source_type
    options = []
    if env and type:
        options = (
            db.query(Datasource)
            .filter(Datasource.env == env, Datasource.type == type)
            .order_by(Datasource.name).all()
        )
    return templates.TemplateResponse(request, "_ds_options.html", {
        "options": options, "env": env, "type": type,
        "field_name": field_name, "with_objects": with_objects == "1",
        "batch": batch == "1",
    })


def _source_objects(ds: Datasource) -> list[str]:
    """从元数据缓存提取可选源对象：kafka->topics，pg->schema.table，
    mongo->db.collection，doris->db.table。"""
    md = ds.metadata_dict or {}
    if ds.type == "kafka":
        return md.get("topics", [])
    if ds.type == "postgresql":
        return [f"{s['name']}.{t['name']}"
                for s in md.get("schemas", []) for t in s.get("tables", [])]
    if ds.type == "mongodb":
        return [f"{d['name']}.{c['name']}"
                for d in md.get("databases", []) for c in d.get("collections", [])]
    if ds.type == "doris":
        return [f"{d['name']}.{t['name']}"
                for d in md.get("databases", []) for t in d.get("tables", [])]
    return []


def _source_dbs(ds: Datasource) -> list[str]:
    """两级级联第一级：库/schema 名列表（pg->schema，mongo/doris->database）。"""
    md = ds.metadata_dict or {}
    if ds.type == "postgresql":
        return [s["name"] for s in md.get("schemas", [])]
    if ds.type in ("mongodb", "doris"):
        return [d["name"] for d in md.get("databases", [])]
    return []


def _source_tables(ds: Datasource, db_name: str) -> list[str]:
    """两级级联第二级：指定库下的表/集合名列表。"""
    md = ds.metadata_dict or {}
    if ds.type == "postgresql":
        for s in md.get("schemas", []):
            if s.get("name") == db_name:
                return [t["name"] for t in s.get("tables", [])]
    elif ds.type == "mongodb":
        for d in md.get("databases", []):
            if d.get("name") == db_name:
                return [c["name"] for c in d.get("collections", [])]
    elif ds.type == "doris":
        for d in md.get("databases", []):
            if d.get("name") == db_name:
                return [t["name"] for t in d.get("tables", [])]
    return []


def _objects_response(request: Request, db: Session, ds_id: int) -> HTMLResponse:
    """源对象选择片段：kafka -> topic + proto 包联动；数据库类 -> 库/表两级级联。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return HTMLResponse('<div class="alert alert-error">数据源不存在</div>')
    if ds.type == "kafka":
        return templates.TemplateResponse(request, "_source_objects.html", {
            "ds": ds,
            "objects": _source_objects(ds),
            "metadata_ready": ds.metadata_status == "ok" and ds.metadata_dict,
        })
    return _dbs_response(request, db, ds_id)


def _dbs_response(request: Request, db: Session, ds_id: int) -> HTMLResponse:
    """库/schema 下拉片段（两级级联第一级）；元数据未就绪降级为手输框。"""
    ds = db.get(Datasource, ds_id)
    if not ds:
        return HTMLResponse('<div class="alert alert-error">数据源不存在</div>')
    return templates.TemplateResponse(request, "_source_dbs.html", {
        "ds": ds,
        "dbs": _source_dbs(ds),
        "metadata_ready": ds.metadata_status == "ok" and ds.metadata_dict,
    })


@router.get("/datasources/objects", response_class=HTMLResponse)
def datasource_objects_qs(request: Request, datasource_id: str = "",
                          db: Session = Depends(get_db)):
    """htmx 联动用（query 参数版本）。"""
    return _objects_response(request, db, _to_int(datasource_id))


@router.get("/datasources/{ds_id}/objects", response_class=HTMLResponse)
def datasource_objects(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """源对象列表（规范路径版本）。"""
    return _objects_response(request, db, ds_id)


@router.get("/datasources/batch-objects", response_class=HTMLResponse)
def datasource_batch_objects(request: Request, datasource_id: str = "",
                             db: Session = Depends(get_db)):
    """批量建作业：源对象多选框列表（带过滤/全选）。"""
    ds = db.get(Datasource, _to_int(datasource_id))
    if not ds:
        return HTMLResponse('<div class="alert alert-error">数据源不存在</div>')
    return templates.TemplateResponse(request, "_batch_objects.html", {
        "ds": ds, "objects": _source_objects(ds),
        "metadata_ready": ds.metadata_status == "ok" and ds.metadata_dict,
    })


@router.get("/datasources/{ds_id}/dbs", response_class=HTMLResponse)
def datasource_dbs(request: Request, ds_id: int, db: Session = Depends(get_db)):
    """库/schema 下拉片段（两级级联第一级）。"""
    return _dbs_response(request, db, ds_id)


@router.get("/datasources/{ds_id}/tables", response_class=HTMLResponse)
def datasource_tables(request: Request, ds_id: int, db: str = "",
                      db_sess: Session = Depends(get_db)):
    """表/集合下拉片段（两级级联第二级）；option 值即 source_ref（库.表）。"""
    ds = db_sess.get(Datasource, ds_id)
    if not ds:
        return HTMLResponse('<div class="alert alert-error">数据源不存在</div>')
    return templates.TemplateResponse(request, "_source_tables.html", {
        "ds": ds, "db_name": db, "tables": _source_tables(ds, db),
    })


# ---------------------------------------------------------------- 目标 Doris 库表（实时查询）

def _doris_dbs_response(request: Request, db: Session, env_name: str) -> HTMLResponse:
    """目标库/表选择片段：实时查环境 Doris；连不上降级为手输框 + 提示。"""
    dbs: list[str] = []
    error = None
    try:
        dbs = doris_ddl.list_doris_dbs(envs.get_env(db, env_name))
    except Exception as e:  # noqa: BLE001 - 不可达时降级，不 500
        error = str(e)[:200]
    return templates.TemplateResponse(request, "_doris_target.html", {
        "dbs": dbs, "error": error,
    })


@router.get("/envs/doris-dbs", response_class=HTMLResponse)
def env_doris_dbs_qs(request: Request, env: str = "", db: Session = Depends(get_db)):
    """htmx 联动用（query 参数版本）。"""
    return _doris_dbs_response(request, db, env)


@router.get("/envs/{name}/doris-dbs", response_class=HTMLResponse)
def env_doris_dbs(request: Request, name: str, db: Session = Depends(get_db)):
    """环境 Doris 库列表（规范路径版本）。"""
    return _doris_dbs_response(request, db, name)


def _doris_tables_options(db: Session, env_name: str, db_name: str) -> HTMLResponse:
    """目标表 datalist 片段（含 datalist 本体，整块替换）；失败时透出错误原因。"""
    from html import escape

    from ..core.crypto import sanitize_error

    tables: list[str] = []
    error = None
    try:
        if env_name and db_name:
            tables = doris_ddl.list_doris_tables(envs.get_env(db, env_name), db_name)
    except Exception as e:  # noqa: BLE001 - 库名非法/不可达/权限等，透出到页面
        error = sanitize_error(str(e))[:200]
    html = '<datalist id="doris-table-list">' + "".join(
        f'<option value="{t}">' for t in tables) + "</datalist>"
    if error:
        html += f'<div class="hint test-fail">表列表查询失败: {escape(error)}</div>'
    return HTMLResponse(html)


@router.get("/envs/doris-tables", response_class=HTMLResponse)
def env_doris_tables_qs(request: Request, env: str = "", db: str = "",
                        db_sess: Session = Depends(get_db)):
    """htmx 联动用（query 参数版本，db 为目标库名）。"""
    return _doris_tables_options(db_sess, env, db)


@router.get("/envs/{name}/doris-tables", response_class=HTMLResponse)
def env_doris_tables(request: Request, name: str, db: str = "",
                     db_sess: Session = Depends(get_db)):
    """环境 Doris 表列表（规范路径版本，?db=库名）。"""
    return _doris_tables_options(db_sess, name, db)


# ---------------------------------------------------------------- 监控数据（JSON / 片段）

@router.get("/jobs/{job_id}/metrics-series")
def job_metrics_series_ep(job_id: int, hours: int = 1, db: Session = Depends(get_db)):
    """作业速率时间序列（Chart.js 数据源）。"""
    job = db.get(Job, job_id)
    if not job:
        return JSONResponse({"error": "作业不存在"}, status_code=404)
    return JSONResponse(monitor.job_metrics_series(db, job, min(hours, 168)))


@router.get("/jobs/{job_id}/logs", response_class=HTMLResponse)
def job_logs_ep(request: Request, job_id: int, db: Session = Depends(get_db)):
    """作业日志片段：平台操作日志（JobEvent 时间线）+ SeaTunnel 侧日志（截尾 500 行）。

    作业失败可能发生在平台层（建表/预检拒绝），此时 SeaTunnel 上没有作业，
    平台操作日志就是唯一的排障依据，必须一并展示。
    """
    job = db.get(Job, job_id)
    if not job:
        return HTMLResponse('<div class="alert alert-error">作业不存在</div>')
    events = (
        db.query(JobEvent)
        .filter(JobEvent.job_id == job.id)
        .order_by(JobEvent.created_at.desc())
        .limit(50)
        .all()
    )
    return templates.TemplateResponse(request, "_job_logs.html", {
        "job": job, "logs": monitor.job_logs(db, job), "events": events,
    })


@router.get("/monitor/env-series")
def env_series_ep(env: str = "", hours: int = 24, db: Session = Depends(get_db)):
    """环境级速率趋势（全部 RUNNING 作业合计，Chart.js 数据源）。"""
    return JSONResponse(monitor.env_metrics_series(db, env, min(hours, 168)))


# ---------------------------------------------------------------- proto 包下拉

@router.get("/protos/options", response_class=HTMLResponse)
def proto_options(db: Session = Depends(get_db)):
    """proto 包 <option> 列表（填充进 select 的 innerHTML）。"""
    pkgs = (
        db.query(ProtoPackage)
        .filter(ProtoPackage.status != "error")
        .order_by(ProtoPackage.name).all()
    )
    opts = ['<option value="">请选择 proto 包</option>']
    opts += [f'<option value="{p.id}">{p.name}</option>' for p in pkgs]
    return HTMLResponse("".join(opts))


def _messages_response(request: Request, db: Session, pkg_id: int,
                       batch: bool = False) -> HTMLResponse:
    """message 下拉片段（选完触发字段映射预览；batch 模式不触发，批量页无单作业映射框）。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return HTMLResponse('<div class="alert alert-error">proto 包不存在</div>')
    return templates.TemplateResponse(request, "_message_select.html", {
        "messages": pkg.top_level_messages, "batch": batch,
    })


@router.get("/protos/messages", response_class=HTMLResponse)
def proto_messages_qs(request: Request, proto_package_id: str = "", batch: str = "",
                      db: Session = Depends(get_db)):
    """htmx 联动用（query 参数版本）。"""
    return _messages_response(request, db, _to_int(proto_package_id), batch == "1")


@router.get("/protos/{pkg_id}/messages", response_class=HTMLResponse)
def proto_messages(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """message 列表（规范路径版本）。"""
    return _messages_response(request, db, pkg_id)


# ---------------------------------------------------------------- 字段映射预览

def _source_columns(ds: Datasource, source_ref: str) -> list[dict] | None:
    """按 source_ref 从元数据缓存中取字段列表 [{"name","type",...}]。"""
    md = ds.metadata_dict or {}
    first, _, second = source_ref.partition(".")
    if ds.type == "postgresql":
        for s in md.get("schemas", []):
            if s.get("name") == first:
                for t in s.get("tables", []):
                    if t.get("name") == second:
                        return t.get("columns", [])
    elif ds.type == "mongodb":
        for d in md.get("databases", []):
            if d.get("name") == first:
                for c in d.get("collections", []):
                    if c.get("name") == second:
                        return c.get("fields", [])
    elif ds.type == "doris":
        for d in md.get("databases", []):
            if d.get("name") == first:
                for t in d.get("tables", []):
                    if t.get("name") == second:
                        return t.get("columns", [])
    return None


def _default_table(source_type: str, source_ref: str) -> str:
    """目标表默认值：{source_type}_{源表名}（源表名为 source_ref 最后一段，非法字符转 _）。"""
    source_name = source_ref.split(".")[-1]
    safe = "".join(c if c.isalnum() or c == "_" else "_" for c in source_name).lower()
    return f"{source_type}_{safe}"


@router.get("/jobs/preview-mapping", response_class=HTMLResponse)
def preview_mapping(request: Request, source_type: str = "", datasource_id: str = "",
                    source_ref: str = "", proto_package_id: str = "",
                    message_name: str = "", env: str = "", add_timestamps: str = "",
                    ttl_column: str = "",
                    db: Session = Depends(get_db)):
    """生成字段映射预览表（Doris 列名/类型可编辑）+ 目标表名默认值。

    目标库/表输入框在第 4 步（_doris_target.html），这里只回映射表；
    add_timestamps 勾选时在末尾附加 kafka_ts（仅 kafka）/etl_time 两列；
    ttl_column 透传用于 oob 刷新 TTL 下拉时保持选中。
    """
    ctx = {"mapping": [], "error": None, "hint": None, "default_table": "",
           "ttl_column": ttl_column, "flatten": set()}

    ds = db.get(Datasource, _to_int(datasource_id))
    if not ds:
        ctx["hint"] = "请先选择数据源"
        return templates.TemplateResponse(request, "_mapping_table.html", ctx)
    if not source_ref:
        ctx["hint"] = "请先选择源对象"
        return templates.TemplateResponse(request, "_mapping_table.html", ctx)

    # 嵌套 message 的拍平选择（flatten_<字段名>=1 由映射表里的下拉回传）
    flatten = {k[len("flatten_"):] for k, v in request.query_params.items()
               if k.startswith("flatten_") and v == "1"}
    ctx["flatten"] = flatten

    # VARIANT 开关取自目标环境 Doris 配置
    variant_enabled = True
    if env and env in envs.env_names(db):
        variant_enabled = bool(envs.get_env(db, env)["doris"].get("variant_enabled", True))

    try:
        if source_type == "kafka":
            pkg = db.get(ProtoPackage, _to_int(proto_package_id))
            if not pkg or not message_name:
                ctx["hint"] = "请继续选择 proto 包与 message，然后自动生成字段映射"
                return templates.TemplateResponse(request, "_mapping_table.html", ctx)
            columns = proto_center.flattened_schema_fields(pkg, message_name, flatten)
        else:
            columns = _source_columns(ds, source_ref)
            if columns is None:
                ctx["error"] = (
                    f"元数据中找不到 {source_ref}，请先到数据源详情页刷新元数据"
                )
                return templates.TemplateResponse(request, "_mapping_table.html", ctx)
        if not columns:
            ctx["error"] = "未获取到任何字段，无法生成映射"
            return templates.TemplateResponse(request, "_mapping_table.html", ctx)
        mapping = build_mapping(source_type, columns, variant_enabled)
        if add_timestamps == "on":
            append_timestamp_columns(mapping, source_type)
        ctx["mapping"] = mapping
    except Exception as e:  # noqa: BLE001 - 预览失败回显错误条，不抛 500
        ctx["error"] = f"生成映射失败: {e}"
        return templates.TemplateResponse(request, "_mapping_table.html", ctx)

    ctx["default_table"] = _default_table(source_type, source_ref)
    return templates.TemplateResponse(request, "_mapping_table.html", ctx)


@router.get("/jobs/batch-mapping", response_class=HTMLResponse)
def batch_mapping(request: Request, p: str = "", db: Session = Depends(get_db)):
    """批量建作业：单对象映射块重渲染（嵌套字段拍平切换）。p 为对象输入名前缀（如 o3_）。

    所需参数（数据源/源对象/proto 等）以 "{p}ds"/"{p}ref" 形式随 hx-include 带上来。
    """
    q = request.query_params
    ds = db.get(Datasource, _to_int(q.get(f"{p}ds")))
    source_type = q.get(f"{p}stype", "")
    source_ref = q.get(f"{p}ref", "")
    env = q.get(f"{p}env", "")
    flatten = {k[len(p) + len("flatten_"):] for k, v in q.items()
               if k.startswith(f"{p}flatten_") and v == "1"}
    variant_enabled = True
    if env and env in envs.env_names(db):
        variant_enabled = bool(envs.get_env(db, env)["doris"].get("variant_enabled", True))
    mapping: list[dict] = []
    error = None
    try:
        if source_type == "kafka":
            pkg = db.get(ProtoPackage, _to_int(q.get(f"{p}pkg")))
            if not pkg or not q.get(f"{p}msg"):
                raise ValueError("缺少 proto 包或 message")
            columns = proto_center.flattened_schema_fields(pkg, q[f"{p}msg"], flatten)
        else:
            columns = _source_columns(ds, source_ref) if ds else None
        if not columns:
            error = "未获取到任何字段，无法生成映射"
        else:
            mapping = build_mapping(source_type, columns, variant_enabled)
            if q.get(f"{p}addts") == "on":
                append_timestamp_columns(mapping, source_type)
    except Exception as e:  # noqa: BLE001 - 预览失败回显错误条，不抛 500
        error = f"生成映射失败: {e}"
    return templates.TemplateResponse(request, "_batch_mapping.html", {
        "p": p, "mapping": mapping, "error": error,
    })
