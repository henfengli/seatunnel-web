"""pytest 公共装置：隔离数据目录 + 干净库 + TestClient。

注意：VISION_DATA_DIR 必须在任何 app 模块导入之前设置（conftest 先于测试模块被导入），
否则会用开发库的 data/ 目录。
"""
import os
import tempfile

os.environ["VISION_DATA_DIR"] = tempfile.mkdtemp(prefix="vision-test-")

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.core.db import Base, SessionLocal, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def fresh_db():
    """每个测试模块一张干净表结构（模块内共享数据，模块间互不影响）。"""
    init_db()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield


@pytest.fixture(scope="module")
def db():
    with SessionLocal() as s:
        yield s


@pytest.fixture(scope="module")
def client():
    # 刻意不走 lifespan（with TestClient(...)）：测试不需要 watchdog 后台轮询
    return TestClient(app)


def check(name, cond, extra=""):
    """兼容原脚本的断言风格：失败时打印检查点名。"""
    assert cond, f"{name} {extra}"
