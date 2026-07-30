"""批量作业操作：BatchTask/BatchItem 落库 + 后台线程串行执行 + 失败聚合告警。

设计：
- 每个作业独立 try/except，单条失败不影响其余，逐条结果写 BatchItem（页面轮询进度）。
- 串行执行：批量场景是维护窗口操作，不把 SeaTunnel master 当压测对象。
- 结束时有失败才发一条聚合钉钉告警，不逐条轰炸。
"""
from __future__ import annotations

import logging
import threading
from datetime import datetime

from sqlalchemy.orm import Session

from ..core.crypto import sanitize_error
from ..core.db import SessionLocal
from ..models import BatchItem, BatchTask, Job
from . import orchestrator

logger = logging.getLogger(__name__)

ACTIONS = ("start", "stop", "restart", "delete", "options")

# options 批量批改支持的高级选项字段（留空 = 不变）
INT_OPTION_KEYS = ("parallelism", "checkpoint_interval", "fetch_max_bytes",
                   "max_poll_records", "buckets")
STR_OPTION_KEYS = ("start_mode", "consumer_group")


def create_batch(db: Session, action: str, jobs: list[Job], params: dict | None = None) -> BatchTask:
    """建任务与逐条记录（PENDING），返回 task；调用方随后 start_batch。"""
    task = BatchTask(action=action, total=len(jobs),
                     params=params or {})
    db.add(task)
    db.flush()
    for job in jobs:
        db.add(BatchItem(batch_id=task.id, job_id=job.id, job_name=job.name))
    db.commit()
    return task


def start_batch(task_id: int) -> None:
    """后台 daemon 线程执行（请求线程立即返回）。"""
    threading.Thread(target=run_batch, args=(task_id,),
                     daemon=True, name=f"batch-{task_id}").start()


def run_batch(task_id: int) -> None:
    """执行任务主体（独立 DB 会话）；幂等：已 DONE 直接返回。

    外层兜底：任何未预期异常（DB 锁、进程内错误）都把任务收敛为 DONE，
    剩余 PENDING 标 FAILED——绝不让任务永远 RUNNING 卡死进度页。
    """
    try:
        _run_batch_inner(task_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("批量任务 #%s 执行异常", task_id)
        try:
            with SessionLocal() as db:
                task = db.get(BatchTask, task_id)
                if task and task.status != "DONE":
                    db.query(BatchItem).filter(
                        BatchItem.batch_id == task_id,
                        BatchItem.status.in_(["PENDING", "RUNNING"])
                    ).update({"status": "FAILED",
                              "detail": f"批量任务异常中断: {str(e)[:300]}"},
                             synchronize_session=False)
                    task.status = "DONE"
                    task.finished_at = datetime.now()
                    db.add(task)
                    db.commit()
        except Exception:  # noqa: BLE001
            logger.exception("批量任务 #%s 异常收敛失败", task_id)


def _run_batch_inner(task_id: int) -> None:
    with SessionLocal() as db:
        task = db.get(BatchTask, task_id)
        if not task or task.status == "DONE":
            return
        task.status = "RUNNING"
        db.add(task)
        db.commit()
        items = (db.query(BatchItem).filter(BatchItem.batch_id == task_id)
                 .order_by(BatchItem.id).all())
        ok = 0
        for i, item in enumerate(items, 1):
            item.status = "RUNNING"
            db.add(item)
            db.commit()
            try:
                status, detail = _apply_one(db, task, item)
            except Exception as e:  # noqa: BLE001 - 单条失败不影响其余
                status, detail = "FAILED", sanitize_error(str(e))[:500]
            item.status = status
            item.detail = detail or None
            ok += 1 if status == "OK" else 0
            task.done = i
            task.ok_count = ok
            db.add_all([item, task])
            db.commit()
        task.status = "DONE"
        task.finished_at = datetime.now()
        db.add(task)
        db.commit()
        _alert_summary(task, items)


def _apply_one(db: Session, task: BatchTask, item: BatchItem) -> tuple[str, str]:
    """执行单个作业，返回 (status, detail)：OK/SKIPPED/FAILED。"""
    action = task.action
    job = db.get(Job, item.job_id)
    if job is None:
        return "FAILED", "作业不存在（可能已被删除）"
    if action == "start":
        if job.status in ("DRAFT", "FAILED", "ERROR"):
            r = orchestrator.submit(db, job)
        elif job.status == "STOPPED":
            r = orchestrator.submit(db, job, start_with_savepoint=True)
        else:
            return "SKIPPED", f"当前状态 {job.status} 不可启动"
    elif action == "stop":
        if job.status != "RUNNING":
            return "SKIPPED", f"当前状态 {job.status} 不可停止（仅 RUNNING）"
        r = orchestrator.stop(db, job, with_savepoint=True)
    elif action == "restart":
        if job.status != "RUNNING":
            return "SKIPPED", f"当前状态 {job.status} 不可重启（仅 RUNNING；其余请用启动）"
        r = orchestrator.update_and_restart(db, job, note=f"batch #{task.id}")
    elif action == "delete":
        return _delete_one(db, job)
    elif action == "options":
        return _options_one(db, task, job)
    else:  # pragma: no cover
        return "FAILED", f"未知操作 {action}"
    if r.get("ok"):
        return "OK", ""
    return "FAILED", str(r.get("error", "未知错误"))[:500]


def _delete_one(db: Session, job: Job) -> tuple[str, str]:
    """批量删除：与单作业删除同一实现（orchestrator.delete），只映射结果形式。"""
    r = orchestrator.delete(db, job)
    if not r.get("ok"):
        return "FAILED", r["error"]
    return "OK", r.get("note", "")


def _options_one(db: Session, task: BatchTask, job: Job) -> tuple[str, str]:
    """批量改配置：merge 高级选项 + 标签；restart 时对 RUNNING 作业接「更新并重启」。"""
    params = task.params
    opts = dict(job.options)
    changed: list[str] = []
    for k in INT_OPTION_KEYS:
        if params.get(k) is not None:
            opts[k] = params[k]
            changed.append(f"{k}={params[k]}")
    for k in STR_OPTION_KEYS:
        if params.get(k):
            opts[k] = params[k]
            changed.append(f"{k}={params[k]}")
    if params.get("tags"):
        job.tags = params["tags"]
        changed.append("标签")
    if not changed:
        return "SKIPPED", "无变更字段"
    job.options = opts
    db.add(job)
    db.commit()
    detail = "已更新: " + ", ".join(changed)
    if not params.get("restart"):
        return "OK", detail + "（重启后生效）"
    if job.status != "RUNNING":
        return "OK", detail + f"；当前状态 {job.status} 未重启"
    r = orchestrator.update_and_restart(db, job, note=f"batch options #{task.id}")
    if not r.get("ok"):
        return "FAILED", detail + f"；重启失败: {str(r.get('error'))[:300]}"
    return "OK", detail + "；已重启生效"


def _alert_summary(task: BatchTask, items: list[BatchItem]) -> None:
    """有失败时聚合一条告警（逐条明细最多列 20 个）。"""
    from .alerting import alert

    failed = [i for i in items if i.status == "FAILED"]
    if not failed:
        return
    lines = [f"### 批量操作部分失败（{task.action}）",
             f"- 任务: #{task.id}，成功 {task.ok_count}/{task.total}",
             "- 失败明细:"]
    lines += [f"  - {i.job_name}: {i.detail}" for i in failed[:20]]
    if len(failed) > 20:
        lines.append(f"  - …等共 {len(failed)} 个")
    alert("\n".join(lines))
