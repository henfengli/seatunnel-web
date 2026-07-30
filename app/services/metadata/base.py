"""元数据自动发现入口：按数据源类型分发，结果落 Datasource 的 metadata_* 字段。"""
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
