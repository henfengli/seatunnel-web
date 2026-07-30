"""数据库会话（SQLite，后台任务串行写库避免写锁）。"""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from .config import DB_URL


class Base(DeclarativeBase):
    pass


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
