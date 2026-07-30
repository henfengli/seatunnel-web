"""后台看护：状态轮询、指标采集、元数据刷新、proto 轮询与钉钉告警。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import or_

from ..core.config import get_settings
from ..core.db import SessionLocal
from ..models import Datasource, Job, JobEvent, MetricSample, ProtoPackage
from . import monitor, orchestrator, proto_center
from .alerting import alert
from .metadata import base as metadata

logger = logging.getLogger(__name__)

scheduler: BackgroundScheduler | None = None


def _poll_status() -> None:
    """轮询 RUNNING/ERROR 作业状态；RUNNING->FAILED 触发告警。

    ERROR 纳入轮询：stop 失败置 ERROR 的作业可能实际还在 SeaTunnel 上跑，需要能刷回 RUNNING；
    UPDATING 不纳入：编排线程自己管理状态，避免 refresh 竞争出假迁移。
    """
    with SessionLocal() as db:
        jobs = db.query(Job).filter(Job.status.in_(["RUNNING", "ERROR"])).all()
        for job in jobs:
            job_id, job_name = job.id, job.name  # 提前取标量，防批量删除竞态下访问已删对象
            try:
                old = job.status
                orchestrator.refresh_status(db, job)
                if old in ("RUNNING", "ERROR") and job.status == "FAILED":
                    alert(
                        f"### 作业失败告警\n"
                        f"- 作业: {job_name}\n- 环境: {job.env}\n"
                        f"- SeaTunnel jobId: {job.seatunnel_job_id}"
                    )
            except Exception as e:  # noqa: BLE001 - 单个失败不影响其他
                db.rollback()  # 批量删除竞态可能产生 StaleDataError，回滚后再记日志
                logger.warning("状态轮询失败 job=%s(%s): %s", job_id, job_name, e)


def _poll_metrics() -> None:
    """采集所有 RUNNING 作业指标。"""
    with SessionLocal() as db:
        jobs = db.query(Job).filter(Job.status == "RUNNING").all()
        for job in jobs:
            try:
                monitor.collect_metrics(db, job)
            except Exception as e:  # noqa: BLE001
                logger.warning("指标采集失败 job=%s(%s): %s", job.id, job.name, e)


def _refresh_metadata() -> None:
    """刷新超过间隔（默认 24h）未更新的数据源元数据；创建/编辑时也会立即刷新。"""
    sec = int(get_settings().watchdog.get("metadata_refresh_seconds", 86400))
    threshold = datetime.now() - timedelta(seconds=sec)
    with SessionLocal() as db:
        ds_list = (
            db.query(Datasource)
            .filter(or_(Datasource.metadata_refreshed_at.is_(None),
                        Datasource.metadata_refreshed_at < threshold))
            .all()
        )
        for ds in ds_list:
            try:
                metadata.refresh(db, ds)
            except Exception as e:  # noqa: BLE001
                logger.warning("元数据刷新失败 ds=%s(%s): %s", ds.id, ds.name, e)


def _prune_metrics() -> None:
    """清理超过保留期（默认 14 天）的指标样本，避免 SQLite 无限增长。"""
    days = int(get_settings().watchdog.get("metrics_retention_days", 14))
    cutoff = datetime.now() - timedelta(days=days)
    with SessionLocal() as db:
        n = (db.query(MetricSample)
             .filter(MetricSample.ts < cutoff)
             .delete(synchronize_session=False))
        db.commit()
    if n:
        logger.info("已清理 %s 条 %d 天前的指标样本", n, days)


def _poll_protos() -> None:
    """轮询到期的 proto 包；status 变 updated 时给引用它的 RUNNING 作业标记 schema 漂移。"""
    now = datetime.now()
    with SessionLocal() as db:
        pkgs = db.query(ProtoPackage).filter(ProtoPackage.source_url != "").all()
        for pkg in pkgs:
            try:
                if pkg.last_polled_at and \
                        (now - pkg.last_polled_at).total_seconds() < pkg.poll_interval_sec:
                    continue
                proto_center.poll_package(db, pkg)
                if pkg.status != "updated":
                    continue
                jobs = (
                    db.query(Job)
                    .filter(Job.proto_package_id == pkg.id, Job.status == "RUNNING")
                    .all()
                )
                for job in jobs:
                    job.status_detail = f"schema 漂移：proto 包 {pkg.name} 已更新"
                    db.add(JobEvent(job_id=job.id, event="alert",
                                    detail=f"proto 包 {pkg.name} 更新，存在 schema 漂移"))
                    db.add(job)
                db.commit()
                if jobs:
                    alert(
                        f"### Proto schema 漂移\n"
                        f"- proto 包: {pkg.name}\n"
                        f"- 受影响作业: {', '.join(j.name for j in jobs)}"
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning("proto 轮询失败 pkg=%s(%s): %s", pkg.id, pkg.name, e)


def start() -> BackgroundScheduler:
    """启动看护调度器（幂等：已启动则直接返回现有实例）。"""
    global scheduler
    if scheduler is not None:
        return scheduler
    wd = get_settings().watchdog
    meta_sec = int(wd.get("metadata_refresh_seconds", 86400))
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(_poll_status, "interval",
                      seconds=int(wd.get("status_poll_seconds", 15)),
                      id="poll_status", max_instances=1, coalesce=True)
    scheduler.add_job(_poll_metrics, "interval",
                      seconds=int(wd.get("metrics_poll_seconds", 60)),
                      id="poll_metrics", max_instances=1, coalesce=True)
    scheduler.add_job(_refresh_metadata, "interval", seconds=meta_sec,
                      id="refresh_metadata", max_instances=1, coalesce=True)
    # proto 包各自有 poll_interval_sec（默认 1h），巡查节拍独立于元数据刷新周期
    scheduler.add_job(_poll_protos, "interval",
                      seconds=int(wd.get("proto_poll_tick_seconds", 3600)),
                      id="poll_protos", max_instances=1, coalesce=True)
    scheduler.add_job(_prune_metrics, "interval", hours=24,
                      id="prune_metrics", max_instances=1, coalesce=True)
    scheduler.start()
    logger.info("watchdog 已启动")
    return scheduler
