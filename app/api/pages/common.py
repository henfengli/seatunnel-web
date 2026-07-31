"""页面路由共享的表单辅助。只放被 2 个以上模块复用的东西，单模块自用的 helper 留在各自文件里。"""
from __future__ import annotations

import re

from fastapi import Request

from ...services.field_mapping import apply_model_ttl
from ...templating import templates

NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
# Doris 类型白名单（防 map_doris_type 注入 DDL）：INT/VARCHAR(10)/DECIMAL(38,10)/DATETIMEV2(3) 等
DORIS_TYPE_RE = re.compile(r"^[A-Z]+(V2)?(\(\d+(,\s*\d+)?\))?$")
# 列默认值白名单：CURRENT_TIMESTAMP/数字/单引号字符串
DEFAULT_RE = re.compile(r"^(CURRENT_TIMESTAMP(\(3\))?|\d+|'([^']|'')*')$")


def form_dict(form) -> dict:
    """Starlette FormData -> 普通 dict（仅保留 str 值），用于校验失败时回显。"""
    return {k: v for k, v in form.items() if isinstance(v, str)}


def form_error(request: Request, template: str, msg: str, **ctx):
    """表单校验失败回显（400）：模板 + 上下文 + error 消息，替代各路由重复的 _err 闭包。"""
    return templates.TemplateResponse(request, template, {**ctx, "error": msg}, status_code=400)


def parse_mapping_form(form, prefix: str = "") -> tuple[list[dict] | None, str | None]:
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
        if not IDENT_RE.match(doris_cols[i]):
            return None, f"Doris 列名非法: {doris_cols[i]}"
        doris_type = doris_types[i].strip().upper() or "STRING"
        if not DORIS_TYPE_RE.match(doris_type):
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
            if not DEFAULT_RE.match(default):
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


def shared_options(form) -> tuple[dict | None, str | None]:
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


def collect_options(form, mapping: list[dict]) -> tuple[dict | None, str | None]:
    """从表单收集高级选项（共享部分 + 模型/TTL）；返回 (options, 错误信息)。create/edit 共用。"""
    options, err = shared_options(form)
    if err:
        return None, err
    err = apply_model_ttl(form.get, mapping, options)
    if err:
        return None, err
    return options, None
