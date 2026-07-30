"""应用核心配置：environments.yaml 加载（环境段仅作首次种子）+ 全局设置。"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parent.parent.parent  # 项目根（seatunnel-web/）
# VISION_DATA_DIR 可覆盖数据目录（测试用 tmp 目录，避免污染开发库）
DATA_DIR = Path(os.environ.get("VISION_DATA_DIR", BASE_DIR / "data"))
ENV_YAML = Path(os.environ.get("VISION_ENV_YAML", BASE_DIR / "environments.yaml"))

DATA_DIR.mkdir(parents=True, exist_ok=True)


class Settings:
    """environments 段仅供 envs.seed_from_yaml 首次种子导入；运行时环境一律读 DB。"""

    def __init__(self) -> None:
        with open(ENV_YAML, encoding="utf-8") as f:
            self._raw: dict[str, Any] = yaml.safe_load(f) or {}
        self.environments: dict[str, dict[str, Any]] = self._raw.get("environments", {})
        self.doris_naming: dict[str, str] = self._raw.get("doris_naming", {})
        self.watchdog: dict[str, Any] = self._raw.get("watchdog", {})

    # ---- 命名规范 ----
    @property
    def default_doris_db(self) -> str:
        return self.doris_naming.get("default_db", "seatunnel_sync")


@lru_cache
def get_settings() -> Settings:
    return Settings()


DB_URL = f"sqlite:///{DATA_DIR / 'vision.db'}"
SECRET_KEY_PATH = DATA_DIR / "secret.key"
