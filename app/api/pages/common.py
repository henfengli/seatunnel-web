"""页面路由共享的表单辅助。只放被 2 个以上模块复用的东西，单模块自用的 helper 留在各自文件里。"""
from __future__ import annotations

import re

from fastapi import Request

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


def apply_model_ttl(get, mapping: list[dict], options: dict) -> str | None:
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


def collect_options(form, mapping: list[dict]) -> tuple[dict | None, str | None]:
    """从表单收集高级选项（共享部分 + 模型/TTL）；返回 (options, 错误信息)。create/edit 共用。"""
    options, err = shared_options(form)
    if err:
        return None, err
    err = apply_model_ttl(form.get, mapping, options)
    if err:
        return None, err
    return options, None
