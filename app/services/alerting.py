"""钉钉自定义机器人告警：webhook 未配置或发送失败仅记日志，绝不阻塞业务。

environments.yaml 的 watchdog 段：
  alert_webhook: "https://oapi.dingtalk.com/robot/send?access_token=XXX"
  alert_secret:  "SEC..."   # 机器人安全设置选「加签」时必填；选「自定义关键词」时留空，
                            # 关键词设为 SeaTunnel 或 告警（标题固定含「SeaTunnel 平台告警」）
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
import urllib.parse

import httpx

from ..core.config import get_settings

logger = logging.getLogger(__name__)


def alert(text: str) -> None:
    """推送 markdown 告警；未配置 webhook 时只记日志。"""
    webhook = get_settings().watchdog.get("alert_webhook") or ""
    if not webhook:
        logger.info("alert(无 webhook): %s", text)
        return
    url = webhook
    secret = get_settings().watchdog.get("alert_secret") or ""
    if secret:
        ts = str(round(time.time() * 1000))
        sign = urllib.parse.quote_plus(base64.b64encode(
            hmac.new(secret.encode(), f"{ts}\n{secret}".encode(),
                     hashlib.sha256).digest()))
        url += f"{'&' if '?' in url else '?'}timestamp={ts}&sign={sign}"
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(url, json={
                "msgtype": "markdown",
                "markdown": {"title": "SeaTunnel 平台告警", "text": text},
            })
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError:
                data = {}
            if data.get("errcode"):
                logger.warning("钉钉告警被拒: %s", data)
            elif not data:
                logger.warning("钉钉告警响应异常（非 JSON）: %s", resp.text[:200])
    except Exception as e:  # noqa: BLE001
        logger.warning("告警发送失败: %s", e)
