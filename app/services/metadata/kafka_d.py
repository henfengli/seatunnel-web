"""Kafka 元数据发现：topic 列表 + 消费组列表。"""
from __future__ import annotations


def _admin_kwargs(conn: dict, request_timeout_ms: int = 8000) -> dict:
    """KafkaAdminClient 连接参数（SASL_* 协议按 kafka-python 参数名透传，PLAIN/SCRAM 均用 sasl_plain_*）。

    conn 为明文连接信息（由调用方解密），健康检查与元数据发现共用。
    """
    kwargs: dict = {
        "bootstrap_servers": conn.get("servers", ""),
        "client_id": "seatunnel-web-metadata",
        "request_timeout_ms": request_timeout_ms,
    }
    protocol = conn.get("security_protocol") or "PLAINTEXT"
    kwargs["security_protocol"] = protocol
    if protocol.startswith("SASL"):
        kwargs["sasl_mechanism"] = conn.get("sasl_mechanism") or "PLAIN"
        kwargs["sasl_plain_username"] = conn.get("sasl_username", "")
        kwargs["sasl_plain_password"] = conn.get("sasl_password", "")
    return kwargs


def discover(conn: dict) -> dict:
    """返回 {"topics": [...], "consumer_groups": [...]}（groups 拉不到则为空）。"""
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(**_admin_kwargs(conn))
    try:
        topics = sorted(t for t in admin.list_topics() if not t.startswith("__"))
        groups: list[str] = []
        try:
            groups = sorted(g[0] if isinstance(g, (tuple, list)) else str(g)
                            for g in admin.list_consumer_groups())
        except Exception:  # noqa: BLE001 - 低版本 broker 不支持则留空
            groups = []
    finally:
        admin.close()
    return {"topics": topics, "consumer_groups": groups}
