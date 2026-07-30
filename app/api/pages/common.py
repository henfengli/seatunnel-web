"""页面路由共享的表单辅助。只放被 2 个以上模块复用的东西，单模块自用的 helper 留在各自文件里。"""
from __future__ import annotations

import re

from fastapi import Request

from ...templating import templates

_NAME_RE = re.compile(r"^[A-Za-z0-9_.\-]{1,128}$")


def _form_dict(form) -> dict:
    """Starlette FormData -> 普通 dict（仅保留 str 值），用于校验失败时回显。"""
    return {k: v for k, v in form.items() if isinstance(v, str)}


def form_error(request: Request, template: str, msg: str, **ctx):
    """表单校验失败回显（400）：模板 + 上下文 + error 消息，替代各路由重复的 _err 闭包。"""
    return templates.TemplateResponse(request, template, {**ctx, "error": msg}, status_code=400)
