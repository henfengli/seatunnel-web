"""测试共享辅助（非 fixture 的放这里；conftest 只留 fixture）。"""


def check(name, cond, extra=""):
    """断言风格检查点：失败时打印检查点名与附加信息。"""
    assert cond, f"{name} {extra}"
