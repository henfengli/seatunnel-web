"""环境管理服务：环境从 DB 读取（Web 可维护），environments.yaml 仅作首次种子。"""
from __future__ import annotations

import re

from sqlalchemy.orm import Session

from ..core.crypto import decrypt, encrypt
from ..models import Environment

_MASTER_SPLIT = re.compile(r"[\s,]+")


def parse_masters(raw: str | None) -> list[str]:
    """masters 原始文本 -> URL 列表（每行一个或逗号分隔，空白忽略）。"""
    return [m for m in _MASTER_SPLIT.split(raw or "") if m]


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
        "proto_site": {
            "base_url": env.proto_site_url or "",
            "auth_header": decrypt(env.proto_site_auth or ""),
        },
    }


def list_envs(db: Session) -> list[Environment]:
    """全部环境（按创建顺序）。"""
    return db.query(Environment).order_by(Environment.id).all()


def env_names(db: Session) -> list[str]:
    return [e.name for e in list_envs(db)]


def get_env(db: Session, name: str) -> dict:
    """按名称取环境配置 dict；不存在抛 KeyError。"""
    env = db.query(Environment).filter(Environment.name == name).first()
    if env is None:
        raise KeyError(f"未定义的环境: {name}（请在「环境」页面创建）")
    return to_dict(env)


def seed_from_yaml(db: Session, settings) -> int:
    """环境表为空时把 YAML 的 environments 段灌入 DB（一次性种子），返回导入条数。"""
    if db.query(Environment).count():
        return 0
    count = 0
    for name, cfg in (settings.environments or {}).items():
        doris = cfg.get("doris", {}) or {}
        proto = cfg.get("proto_site", {}) or {}
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
            proto_site_url=proto.get("base_url") or None,
            proto_site_auth=encrypt(proto["auth_header"]) if proto.get("auth_header") else None,
        ))
        count += 1
    db.commit()
    return count
