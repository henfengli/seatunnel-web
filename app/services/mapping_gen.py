"""字段映射自动生成：作业表单兜底、批量建作业、映射预览共用这一条路径。

kafka 源走 proto 包（可选拍平嵌套 message），其余源走数据源元数据缓存；
统一 append 时间戳列、统一 variant_enabled 读取，避免四处复制规则漂移。
field_mapping 保持纯函数库，涉及 DB/环境/元数据的编排都在这里。
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from ..models import Datasource, ProtoPackage
from . import envs, proto_center
from .field_mapping import append_timestamp_columns, build_mapping
from .metadata import base as metadata


def auto_mapping(db: Session, env: str, source_type: str, ds: Datasource | None,
                 source_ref: str, pkg: ProtoPackage | None = None,
                 message_name: str | None = None, add_timestamps: bool = False,
                 flatten=frozenset()) -> list[dict] | None:
    """按数据源元数据/proto 自动生成字段映射；元数据缺失/解析异常返回 None（调用方决定报错文案）。"""
    # VARIANT 开关取自目标环境 Doris 配置；预览页环境可能还没选，缺省 True
    variant_enabled = True
    if env and env in envs.env_names(db):
        variant_enabled = bool(envs.get_env(db, env)["doris"].get("variant_enabled", True))
    try:
        if source_type == "kafka":
            columns = proto_center.flattened_schema_fields(pkg, message_name, flatten)
        else:
            columns = metadata.source_columns(ds, source_ref) if ds else None
    except Exception:  # noqa: BLE001 - proto 包 error 状态/parsed 为空等，按"生成失败"回显
        return None
    if not columns:
        return None
    mapping = build_mapping(source_type, columns, variant_enabled)
    if add_timestamps:
        append_timestamp_columns(mapping, source_type)
    return mapping
