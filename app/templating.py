"""页面模板引擎与 Web 层公共工具（Jinja2 过滤器、跳转/flash 辅助）。"""
from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urlencode

from fastapi import Request, Response
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from .core.config import BASE_DIR
from .core.crypto import decrypt_safe
from .core.db import SessionLocal
from .services import envs

templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates" / "pages"))


def _fmt_dt(value) -> str:
    """时间展示：None -> "-"，其余精确到秒。"""
    if not value:
        return "-"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _ds_addr(ds) -> str:
    """数据源地址摘要（列表页用）。"""
    conn = ds.connection if hasattr(ds, "connection") else (ds or {})
    if ds.type == "kafka":
        return conn.get("servers", "")
    if ds.type == "doris":
        return conn.get("fenodes", "")
    host = conn.get("host", "")
    port = conn.get("port", "")
    return f"{host}:{port}" if port else host


def _human_bytes(n) -> str:
    """字节量人性化展示。"""
    try:
        n = float(n or 0)
    except (TypeError, ValueError):
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}TB"


_CONF_SECRET_LINE = re.compile(
    r'(?m)^(\s*(?:[\w.\-]*password[\w.\-]*|sasl\.jaas\.config)\s*=\s*)".*"(;?)\s*$',
    re.IGNORECASE)
_MONGO_URI_PWD = re.compile(r'(mongodb://[^:("]+:)[^@"]+(@)')


def _mask_conf(text: str) -> str:
    """conf 展示前掩码：password / sasl.jaas.config 行与 mongo uri 内嵌密码替换为 ****。"""
    if not text:
        return text
    text = _CONF_SECRET_LINE.sub(r'\1"****"\2', text)
    return _MONGO_URI_PWD.sub(r'\1****\2', text)


templates.env.filters["dt"] = _fmt_dt
templates.env.filters["ds_addr"] = _ds_addr
templates.env.filters["human_bytes"] = _human_bytes
templates.env.filters["mask_conf"] = _mask_conf
# 解密（conf 加密落库后的展示还原；历史明文行兼容）
templates.env.filters["dec"] = decrypt_safe
# 自定义 test：doris 列类型是否日期/时间类型（TTL 下拉过滤用；
# 注意 Jinja2 标准库没有 match test，Ansible 才有）
templates.env.tests["datetype"] = lambda v: str(v).startswith(("DATE", "DATETIME"))

def _all_env_names() -> list[str]:
    """模板全局：环境名列表（读 DB；表未就绪等异常时返回空，避免渲染崩溃）。"""
    try:
        with SessionLocal() as db:
            return envs.env_names(db)
    except Exception:  # noqa: BLE001
        return []


# 模板全局函数：base 顶部环境标识等场景使用（延迟求值，避免模块导入即读库）
# 命名为 all_env_names，避免与各页面 context 中的 env_names 列表冲突
templates.env.globals["all_env_names"] = _all_env_names


def goto(request: Request, url: str, msg: str | None = None, ok: bool = True) -> Response:
    """统一跳转：带 flash 消息（query 参数）；htmx 请求用 HX-Redirect 头。"""
    if msg:
        sep = "&" if "?" in url else "?"
        url = url + sep + urlencode({"msg": msg, "msg_type": "success" if ok else "error"})
    if request.headers.get("HX-Request"):
        # htmx 不跟随 30x，需用 HX-Redirect 响应头让浏览器跳转
        return Response(status_code=200, headers={"HX-Redirect": url})
    return RedirectResponse(url, status_code=303)
