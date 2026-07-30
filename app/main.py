"""应用入口：FastAPI 实例、静态资源、路由注册、启动钩子。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

import uvicorn
from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from .api import json_api, pages
from .core.config import BASE_DIR, get_settings
from .core.db import SessionLocal, init_db
from .models import BatchItem, BatchTask
from .services import envs, watchdog

# APScheduler 每 15 秒一条任务执行日志太吵，只看 WARNING 及以上
logging.getLogger("apscheduler").setLevel(logging.WARNING)


def _sweep_stuck_batches(db) -> None:
    """启动收敛：上次进程退出时仍在 RUNNING 的批量任务标记为中断（防进度页无限轮询）。"""
    stuck = db.query(BatchTask).filter(BatchTask.status == "RUNNING").all()
    for task in stuck:
        db.query(BatchItem).filter(
            BatchItem.batch_id == task.id,
            BatchItem.status.in_(["PENDING", "RUNNING"])).update(
            {"status": "FAILED", "detail": "服务重启，批量任务中断"},
            synchronize_session=False)
        task.status = "DONE"
        task.finished_at = datetime.now()
        db.add(task)
    if stuck:
        db.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动：建表 + 环境种子（表为空时从 YAML 导入一次）+ 中断批量任务收敛 + 后台看护
    init_db()
    with SessionLocal() as db:
        envs.seed_from_yaml(db, get_settings())
        _sweep_stuck_batches(db)
    watchdog.start()
    yield


app = FastAPI(title="SeaTunnel 作业管理平台", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")

app.include_router(pages.router)
app.include_router(json_api.router)


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    """无图标，返回 204 消除浏览器 404 噪音。"""
    return Response(status_code=204)


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
