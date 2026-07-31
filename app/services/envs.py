"""环境管理服务：环境从 DB 读取（Web 可维护），environments.yaml 仅作首次种子。"""
from __future__ import annotations

import re

from sqlalchemy import func
from sqlalchemy.orm import Session

from ..core.config import DATA_DIR
from ..core.crypto import decrypt, encrypt
from ..models import Environment, Job

_MASTER_SPLIT = re.compile(r"[\s,]+")


def parse_masters(raw: str | None) -> list[str]:
    """masters 原始文本 -> URL 列表（每行一个或逗号分隔，空白忽略）。"""
    return [m for m in _MASTER_SPLIT.split(raw or "") if m]


def master_hosts(env: Environment) -> list[str]:
    """masters 的 host:port 列表（展示用，去 scheme）。"""
    return [m.split("://")[-1] for m in parse_masters(env.seatunnel_masters)]


def to_dict(env: Environment) -> dict:
    """转成与旧 YAML 相同的形状，供服务层使用（密码字段在此解密）。"""
    return {
        "seatunnel": {"masters": parse_masters(env.seatunnel_masters)},
        "doris": {
            "fenodes": env.doris_fenodes,
            "query_port": env.doris_query_port,
            "username": env.doris_username,
            "password": decrypt(env.doris_password or ""),
            "variant_enabled": bool(env.variant_enabled),
            "default_buckets": env.default_buckets,
            "replication_num": env.replication_num,
        },
    }


def list_envs(db: Session) -> list[Environment]:
    """全部环境（按创建顺序）。"""
    return db.query(Environment).order_by(Environment.id).all()


def env_names(db: Session) -> list[str]:
    return [e.name for e in list_envs(db)]


def job_status_counts(db: Session) -> dict[str, dict[str, int]]:
    """一次 GROUP BY 聚合 {env: {status: 数量}}，供总览/环境列表共用，避免逐环境多次 COUNT。"""
    counts: dict[str, dict[str, int]] = {}
    for env_name, status, cnt in (
            db.query(Job.env, Job.status, func.count()).group_by(Job.env, Job.status).all()):
        counts.setdefault(env_name, {})[status] = cnt
    return counts


def get_env(db: Session, name: str) -> dict:
    """按名称取环境配置 dict；不存在抛 KeyError。"""
    env = db.query(Environment).filter(Environment.name == name).first()
    if env is None:
        raise KeyError(f"未定义的环境: {name}（请在「环境」页面创建）")
    return to_dict(env)


def seed_from_yaml(db: Session, settings) -> int:
    """首次启动把 YAML 的 environments 段灌入 DB（一次性种子），返回导入条数。

    用数据目录下的标记文件判定"首次"，而不是"环境表为空"——用户在 Web 上删光
    环境后重启，不应把 YAML 种子复活回来。标记在 DATA_DIR 里，随备份一起拷贝。
    """
    marker = DATA_DIR / ".env_seeded"
    if marker.exists():
        return 0
    count = 0
    # 老库升级：已有环境说明种子早就导入过，只补标记，不重复导入
    if db.query(Environment).count():
        marker.touch()
        return 0
    for name, cfg in (settings.environments or {}).items():
        doris = cfg.get("doris", {}) or {}
        masters = (cfg.get("seatunnel", {}) or {}).get("masters", []) or []
        db.add(Environment(
            name=name,
            seatunnel_masters="\n".join(masters),
            doris_fenodes=doris.get("fenodes", "") or "",
            doris_query_port=int(doris.get("query_port", 9030)),
            doris_username=doris.get("username", "root") or "root",
            doris_password=encrypt(doris.get("password", "") or ""),
            variant_enabled=bool(doris.get("variant_enabled", True)),
            default_buckets=int(doris.get("default_buckets", 10)),
            replication_num=int(doris.get("replication_num", 1)),
        ))
        count += 1
    db.commit()
    marker.touch()
    return count
