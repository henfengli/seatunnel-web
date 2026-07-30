"""SeaTunnel Zeta REST 客户端：平台与 SeaTunnel 集群的唯一 HTTP 通道。

orchestrator / monitor / pages / batch_ops 全部走这里，不要各自再发 HTTP。
本模块不做任何 DB 写操作；db 参数仅用于按环境名解析 master 列表。
"""
from __future__ import annotations

import time

import httpx
from sqlalchemy.orm import Session

from . import envs

TIMEOUT = 10
TERMINAL_STATES = ("FINISHED", "CANCELED", "FAILED")


def request_env(env_dict: dict, method: str, path: str, retry: bool = True,
                raw: bool = False, timeout: int = TIMEOUT, **kw):
    """按顺序尝试各 master，全部失败抛最后一个异常。

    HTTP 错误时把 SeaTunnel 返回的错误正文附在异常里（否则 500 只剩状态码，无法排查）。
    retry=False 只试第一个 master——非幂等请求（submit-job）跨 master 重试可能产生重复作业。
    raw=True 返回响应文本（日志类接口），否则解析 JSON（空响应返回 {}）。
    """
    last: Exception | None = None
    masters = env_dict["seatunnel"]["masters"]
    for master in (masters if retry else masters[:1]):
        url = master.rstrip("/") + path
        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.request(method, url, **kw)
                if resp.status_code >= 400:
                    body = resp.text[:500].strip()
                    raise RuntimeError(f"SeaTunnel 返回 {resp.status_code}: {body}")
                if raw:
                    return resp.text
                return resp.json() if resp.content else {}
        except Exception as e:  # noqa: BLE001 - 换下一个 master 重试
            last = e
    raise last  # type: ignore[misc]


def request(db: Session, env: str, method: str, path: str, **kw):
    """按环境名解析配置后调 request_env。"""
    return request_env(envs.get_env(db, env), method, path, **kw)


def get(db: Session, env: str, path: str, **kw) -> dict:
    return request(db, env, "GET", path, **kw)


def post(db: Session, env: str, path: str, **kw) -> dict:
    return request(db, env, "POST", path, **kw)


def submit_conf(db: Session, env: str, conf: str, extra_params: dict | None = None) -> dict:
    """提交 HOCON 配置到 /submit-job（不跨 master 重试：非幂等，重试可能重复提交）。"""
    params = {"format": "hocon", **(extra_params or {})}
    return request(db, env, "POST", "/submit-job", retry=False, params=params, content=conf,
                   headers={"Content-Type": "text/plain; charset=utf-8"})


def job_info(db: Session, env: str, job_id: str) -> dict | None:
    """GET /job-info/{id}，失败回退 /job-info?jobId=；都失败返回 None。"""
    try:
        return get(db, env, f"/job-info/{job_id}")
    except Exception:  # noqa: BLE001
        try:
            return get(db, env, "/job-info", params={"jobId": job_id})
        except Exception:  # noqa: BLE001
            return None


def wait_terminal(db: Session, env: str, job_id: str, timeout_sec: int = 60,
                  interval_sec: int = 2) -> str:
    """轮询 job-info 直到终态或超时，返回最后看到的 SeaTunnel 状态。"""
    deadline = time.time() + timeout_sec
    status = ""
    while time.time() < deadline:
        info = job_info(db, env, job_id)
        status = str((info or {}).get("jobStatus", "")).upper()
        if status in TERMINAL_STATES:
            break
        time.sleep(interval_sec)
    return status
