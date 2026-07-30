"""小格式化工具：模板过滤器与监控展示共用（放 core 避免 services → templating 反向依赖）。"""
from __future__ import annotations


def human_bytes(n) -> str:
    """字节量人性化展示（"2048000" -> "2.0MB"）；非法输入返回 "-"。"""
    try:
        n = float(n or 0)
    except (TypeError, ValueError):
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}TB"
