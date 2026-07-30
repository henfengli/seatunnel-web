"""连接健康测试：保存时自动测一次（失败不阻塞保存），也可从表单/详情页手动触发。

结果落 Datasource.health_status/health_detail、Environment.health，
列表页以圆点展示（ok 绿 / fail 红 / unknown 灰）。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from ..core.crypto import decrypt_conn, sanitize_error
from ..models import Datasource, Environment


# ---------------------------------------------------------------- 数据源

def _test_kafka(conn: dict) -> tuple[bool, str]:
    import socket

    from kafka import KafkaAdminClient

    from .metadata.kafka_d import _admin_kwargs

    # 先做 socket 级预检：任一 broker 可达即过（KafkaAdminClient 的 bootstrap 探测默认要等 30s）
    reachable = False
    first_err = ""
    for srv in (conn.get("servers") or "").split(","):
        srv = srv.strip()
        if not srv:
            continue
        host, sep, port = srv.rpartition(":")
        if not sep:  # 无端口条目：整段是 host，默认 9092
            host, port = srv, "9092"
        try:
            with socket.create_connection((host, int(port)), timeout=5):
                reachable = True
                break
        except (OSError, ValueError) as e:
            first_err = first_err or f"无法连接 {srv}: {e}"
    if not reachable:
        return False, first_err or "servers 为空"

    admin = KafkaAdminClient(**_admin_kwargs(conn, request_timeout_ms=5000))
    try:
        return True, f"连接成功（{len(admin.list_topics())} 个 topic）"
    finally:
        admin.close()


def _test_mongodb(conn: dict) -> tuple[bool, str]:
    from pymongo import MongoClient

    from .metadata.mongo_d import _build_uri

    client = MongoClient(_build_uri(conn), serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    try:
        info = client.server_info()
        return True, f"连接成功（MongoDB {info.get('version', '?')}）"
    finally:
        client.close()


def _test_postgresql(conn: dict) -> tuple[bool, str]:
    import psycopg2

    with psycopg2.connect(
        host=conn.get("host", "localhost"),
        port=int(conn.get("port", 5432)),
        dbname=conn.get("db") or conn.get("database"),
        user=conn.get("username") or conn.get("user"),
        password=conn.get("password", ""),
        connect_timeout=5,
    ) as c:
        with c.cursor() as cur:
            cur.execute("SELECT 1")
    return True, "连接成功"


def _test_doris(conn: dict) -> tuple[bool, str]:
    import pymysql

    c = pymysql.connect(
        host=conn.get("host") or conn.get("fenodes", "").split(",")[0].split(":")[0] or "localhost",
        port=int(conn.get("port") or conn.get("query_port") or 9030),
        user=conn.get("username") or conn.get("user") or "root",
        password=conn.get("password", ""),
        connect_timeout=5,
        read_timeout=10,
        charset="utf8mb4",
    )
    try:
        with c.cursor() as cur:
            cur.execute("SELECT 1")
        return True, "连接成功"
    finally:
        c.close()


_TESTERS = {
    "kafka": _test_kafka,
    "mongodb": _test_mongodb,
    "postgresql": _test_postgresql,
    "doris": _test_doris,
}


def test_datasource(ds_type: str, conn: dict) -> tuple[bool, str]:
    """按类型测连通性（conn 为明文连接信息）；返回 (是否通, 成功/错误信息)。不抛异常。"""
    tester = _TESTERS.get(ds_type)
    if tester is None:
        return False, f"未知数据源类型: {ds_type}"
    try:
        return tester(conn)
    except Exception as e:  # noqa: BLE001
        return False, sanitize_error(str(e))[:500]


# ---------------------------------------------------------------- 环境

def test_environment(env_dict: dict) -> dict:
    """测环境的 SeaTunnel（任一 master /overview 通即通）与 Doris（SELECT 1）。

    env_dict 为 services.envs.to_dict 的形状；返回 {"seatunnel": (ok,msg), "doris": (ok,msg)}。
    """
    import httpx

    st_ok, st_msg = False, "无可用 master"
    for master in env_dict.get("seatunnel", {}).get("masters", []):
        try:
            resp = httpx.get(master.rstrip("/") + "/overview", timeout=3)
            resp.raise_for_status()
            st_ok, st_msg = True, f"连接成功（{master}）"
            break
        except Exception as e:  # noqa: BLE001 - 换下一个 master
            st_msg = str(e)[:300]
    doris_ok, doris_msg = _test_doris_safe(env_dict.get("doris", {}))
    return {"seatunnel": (st_ok, st_msg), "doris": (doris_ok, doris_msg)}


def _test_doris_safe(doris: dict) -> tuple[bool, str]:
    try:
        return _test_doris(doris)
    except Exception as e:  # noqa: BLE001
        return False, sanitize_error(str(e))[:500]


# ---------------------------------------------------------------- 落库与聚合

def check_datasource(db: Session, ds: Datasource) -> None:
    """测一次并写入 health 字段；任何异常都记为 fail，不阻塞调用方。"""
    try:
        ok, msg = test_datasource(ds.type, decrypt_conn(ds.connection))
    except Exception as e:  # noqa: BLE001
        ok, msg = False, str(e)[:500]
    ds.health_status = "ok" if ok else "fail"
    ds.health_detail = msg
    db.add(ds)
    db.commit()


def check_environment(db: Session, env: Environment) -> None:
    """测一次并写入 health；不抛异常。"""
    from . import envs

    try:
        res = test_environment(envs.to_dict(env))
        payload = {
            "seatunnel": {"ok": res["seatunnel"][0], "msg": sanitize_error(res["seatunnel"][1])},
            "doris": {"ok": res["doris"][0], "msg": sanitize_error(res["doris"][1])},
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
    except Exception as e:  # noqa: BLE001
        payload = {
            "seatunnel": {"ok": False, "msg": sanitize_error(str(e))[:300]},
            "doris": {"ok": False, "msg": sanitize_error(str(e))[:300]},
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
    env.health = payload
    db.add(env)
    db.commit()


def env_aggregate(health: dict | None) -> tuple[str, str]:
    """环境健康聚合：任一 fail -> fail，全 ok -> ok，未测 -> unknown；返回 (status, title 多行文本)。"""
    data = health or None
    if not data:
        return "unknown", "未测试"
    status = "ok"
    lines = []
    for key, label in (("seatunnel", "SeaTunnel"), ("doris", "Doris")):
        item = data.get(key) or {}
        ok = bool(item.get("ok"))
        if not ok:
            status = "fail"
        lines.append(f"{label}: {'通' if ok else '不通'}（{item.get('msg', '')}）")
    if data.get("checked_at"):
        lines.append(f"测试时间: {data['checked_at']}")
    return status, "\n".join(lines)


def env_health_parts(health: dict | None) -> dict:
    """拆分 SeaTunnel/Doris 各自的健康状态：{"seatunnel": (status, msg), "doris": (status, msg)}。

    status = ok/fail/unknown（对应列表页圆点样式）。
    """
    data = health or None
    parts = {}
    for key in ("seatunnel", "doris"):
        if not data or key not in data:
            parts[key] = ("unknown", "未测试")
        else:
            item = data.get(key) or {}
            parts[key] = ("ok" if item.get("ok") else "fail", str(item.get("msg", "")))
    return parts
