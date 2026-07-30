"""元数据自动发现入口：按数据源类型分发，结果落 Datasource 的 metadata_* 字段。"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from ...core.crypto import decrypt, sanitize_error
from ...models import Datasource
from . import doris_d, kafka_d, mongo_d, pg_d

_DISCOVERERS = {
    "kafka": kafka_d.discover,
    "mongodb": mongo_d.discover,
    "postgresql": pg_d.discover,
    "doris": doris_d.discover,
}


def _decrypted_connection(ds: Datasource) -> dict:
    """connection_json 中 key 含 password/auth 的值是密文，解密后交给 discoverer。"""
    conn = {}
    for k, v in ds.connection.items():
        if isinstance(v, str) and v and ("password" in k.lower() or "auth" in k.lower()):
            try:
                v = decrypt(v)
            except Exception:  # noqa: BLE001 - 非密文则按原文使用
                pass
        conn[k] = v
    return conn


def refresh(db: Session, ds: Datasource) -> Datasource:
    """刷新单个数据源的元数据缓存；失败记 status=error，不抛异常。"""
    try:
        discoverer = _DISCOVERERS[ds.type]
        result = discoverer(_decrypted_connection(ds))
        ds.metadata_json = ds._dumps(result)
        ds.metadata_status = "ok"
        ds.metadata_error = None
    except Exception as e:  # noqa: BLE001
        ds.metadata_status = "error"
        ds.metadata_error = sanitize_error(str(e))[:2000]
    ds.metadata_refreshed_at = datetime.now()
    db.add(ds)
    db.commit()
    return ds
