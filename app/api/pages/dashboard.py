"""总览与环境级监控面板。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from ...core.db import get_db
from ...models import Datasource, Job, JobEvent, ProtoPackage
from ...services import envs, health, monitor
from ...templating import templates

router = APIRouter()


# ---------------------------------------------------------------- 总览

@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    counts = envs.job_status_counts(db)
    env_cards = []
    for env in envs.list_envs(db):
        c = counts.get(env.name, {})
        h_status, h_title = health.env_aggregate(env.health)
        env_cards.append({
            "id": env.id,
            "name": env.name,
            "masters": envs.master_hosts(env),
            "fenodes": env.doris_fenodes,
            "job_count": sum(c.values()),
            "running_count": c.get("RUNNING", 0),
            "failed_count": c.get("FAILED", 0) + c.get("ERROR", 0),
            "health": h_status,
            "health_title": h_title,
        })
    recent_events = (
        db.query(JobEvent).order_by(JobEvent.created_at.desc()).limit(10).all()
    )
    bad_jobs = db.query(Job).filter(Job.status.in_(["FAILED", "ERROR"])).all()
    total = {st: sum(c.get(st, 0) for c in counts.values())
             for st in ("DRAFT", "RUNNING", "FAILED", "ERROR", "STOPPED", "UPDATING")}
    stats = {
        "jobs": sum(total.values()),
        "running": total["RUNNING"],
        "failed": len(bad_jobs),
        "stopped": total["STOPPED"],
        "datasources": db.query(Datasource).count(),
        "protos": db.query(ProtoPackage).count(),
    }
    return templates.TemplateResponse(request, "dashboard.html", {
        "active": "dashboard",
        "env_cards": env_cards,
        "recent_events": recent_events,
        "bad_jobs": bad_jobs,
        "stats": stats,
    })


# ---------------------------------------------------------------- 监控面板

@router.get("/monitor", response_class=HTMLResponse)
def monitor_page(request: Request, env: str = "", db: Session = Depends(get_db)):
    """环境级监控：SeaTunnel/Doris 集群状态 + 作业汇总 + 速率趋势图。"""
    names = envs.env_names(db)
    cur = env if env in names else (names[0] if names else "")
    st = monitor.seatunnel_cluster(db, cur) if cur else None
    dr = monitor.doris_cluster(db, cur) if cur else None
    jobs = db.query(Job).filter(Job.env == cur).all() if cur else []
    job_stats = {
        "total": len(jobs),
        "running": sum(1 for j in jobs if j.status == "RUNNING"),
        "failed": sum(1 for j in jobs if j.status in ("FAILED", "ERROR")),
        "stopped": sum(1 for j in jobs if j.status == "STOPPED"),
    }
    return templates.TemplateResponse(request, "capacity.html", {
        "active": "monitor", "env_names": names, "cur_env": cur,
        "st": st, "dr": dr, "job_stats": job_stats,
    })


@router.get("/capacity", response_class=HTMLResponse)
def capacity_redirect(request: Request):
    """老路由重定向到 /monitor。"""
    return RedirectResponse("/monitor", status_code=302)


