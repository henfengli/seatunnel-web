"""元数据自动发现入口：按数据源类型分发，结果落 Datasource 的 metadata_* 字段。

新源类型接入清单（如 MySQL 源 / ClickHouse sink），需要触达的点位：
1. models.DS_TYPES                          类型枚举
2. api/pages/datasource.py 表单/_DS_REQUIRED 连接字段
3. health._TESTERS                          连接测试
4. metadata/_DISCOVERERS（本文件）+ 新建 <type>_d.py 发现模块
5. render._TEMPLATES                        SeaTunnel conf 模板
6. field_mapping.build_mapping              类型映射 if-elif
7. metadata/base.py 的 source_* 导航函数     库/表/列浏览 if-elif
8. templating._ds_addr                      列表页地址展示
9. 表单模板（datasource_form.html 等）       类型显隐联动
10. render.render_conf 的 mongo_cdc 特判     cdc 模板选择/enable_delete 分支
11. 批量向导模板（job_batch_form.html）      逐对象表单的类型显隐
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from ...core.crypto import decrypt_conn, sanitize_error
from ...models import Datasource
from . import doris_d, kafka_d, mongo_d, pg_d

_DISCOVERERS = {
    "kafka": kafka_d.discover,
    "mongodb": mongo_d.discover,
    "postgresql": pg_d.discover,
    "doris": doris_d.discover,
}



def refresh(db: Session, ds: Datasource) -> Datasource:
    """刷新单个数据源的元数据缓存；失败记 status=error，不抛异常。"""
    try:
        discoverer = _DISCOVERERS[ds.type]
        result = discoverer(decrypt_conn(ds.connection))
        ds.metadata_dict = result
        ds.metadata_status = "ok"
        ds.metadata_error = None
    except Exception as e:  # noqa: BLE001
        ds.metadata_status = "error"
        ds.metadata_error = sanitize_error(str(e))[:2000]
    ds.metadata_refreshed_at = datetime.now()
    db.add(ds)
    db.commit()
    return ds


# ---------------------------------------------------------------- 元数据缓存导航（只读）


def source_objects(ds: Datasource) -> list[str]:
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


def source_dbs(ds: Datasource) -> list[str]:
    """两级级联第一级：库/schema 名列表（pg->schema，mongo/doris->database）。"""
    md = ds.metadata_dict or {}
    if ds.type == "postgresql":
        return [s["name"] for s in md.get("schemas", [])]
    if ds.type in ("mongodb", "doris"):
        return [d["name"] for d in md.get("databases", [])]
    return []


def source_tables(ds: Datasource, db_name: str) -> list[str]:
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


def source_columns(ds: Datasource, source_ref: str) -> list[dict] | None:
    """按 source_ref 从元数据缓存中取字段列表 [{"name","type",...}]；找不到返回 None。"""
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
