"""数据库会话（SQLite，后台任务串行写库避免写锁）+ JsonDict 列类型。"""
from __future__ import annotations

import json

from sqlalchemy import Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.types import TypeDecorator

from .config import DB_URL


class Base(DeclarativeBase):
    pass


class JsonDict(TypeDecorator):
    """TEXT 列存 JSON，Python 侧直接读写 dict/list，免去各模型手写 *_json + property 样板。

    数据库列名保持原 *_json 不变（兼容已有库）；NULL 读出时返回 default()。
    """

    impl = Text
    cache_ok = True

    def __init__(self, default=dict):
        super().__init__()
        self._default = default

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False)

    def process_result_value(self, value, dialect):
        if value is None:
            return self._default()
        return json.loads(value)


engine = create_engine(DB_URL, connect_args={"check_same_thread": False, "timeout": 30})
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_db() -> None:
    from .. import models  # noqa: F401  确保模型已注册

    Base.metadata.create_all(engine)
    # WAL：读写不互斥，缓解后台任务（watchdog）与 Web 请求并发写锁
    with engine.begin() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
