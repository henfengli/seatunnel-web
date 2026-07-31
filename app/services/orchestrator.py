"""提交/停止/更新编排：SeaTunnel REST（seatunnel_client）+ Doris DDL + 版本留档 + 事件记录。

约定：
- 公开编排函数返回 {"ok": bool, ...} dict，不抛异常；错误落 Job.status/JobEvent。
- 事务约定：每个公开编排函数内部自行 commit（编排中间态对进度页可见是刻意的），
  调用方（Web 路由 / batch_ops）不要指望外层事务包裹。
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from ..core.crypto import decrypt_safe, sanitize_error
from ..models import Job, JobEvent, JobVersion, MetricSample
from . import doris_ddl, envs, render, seatunnel_client as st

# SeaTunnel 作业状态 -> 管理端状态（其余状态不改动）
_STATUS_MAP = {"RUNNING": "RUNNING", "FAILED": "FAILED", "FINISHED": "STOPPED", "CANCELED": "STOPPED"}


def _event(db: Session, job: Job, event: str, detail: str = "") -> None:
    db.add(JobEvent(job_id=job.id, event=event, detail=detail))


def _fail(db: Session, job: Job, event: str, message: str) -> dict:
    """统一失败处理：status=ERROR + 事件落库（消息先脱敏，防驱动异常带出连接串密码）。"""
    message = sanitize_error(message)
    job.status = "ERROR"
    job.status_detail = message[:2000]
    _event(db, job, event, job.status_detail)
    db.add(job)
    db.commit()
    return {"ok": False, "error": message}


def _find_running_conflict(db: Session, job: Job) -> Job | None:
    """防双作业重复消费：查同 env + 同数据源 + 同源对象的 RUNNING 作业（排除自身）。"""
    return (
        db.query(Job)
        .filter(Job.env == job.env, Job.datasource_id == job.datasource_id,
                Job.source_ref == job.source_ref, Job.status == "RUNNING",
                Job.id != job.id)
        .first()
    )


def _conflict_error(conflict: Job) -> str:
    return (f"已有运行中的作业 {conflict.name} 在消费同一源（同环境+同数据源+同 topic/表），"
            f"不同作业会各自全量消费导致数据写双份。请先停止该作业。")


def _needs_recreate_msg(reasons: list[str]) -> str:
    return ("目标表已存在且与当前配置冲突，需重建表后才能继续："
            + "；".join(reasons)
            + "。请到作业详情页点「重建目标表」（可选删表重建或数据迁移重建）")


#: 允许提交的状态（白名单）：RUNNING/UPDATING 等一律拒绝，防线不依赖调用方
SUBMITTABLE_STATES = ("DRAFT", "FAILED", "ERROR", "STOPPED")


def submit(db: Session, job: Job, start_with_savepoint: bool = False) -> dict:
    """状态白名单 + CAS 防并发提交 -> 防重复消费 -> savepoint 就绪检查 -> 建表 -> 渲染留档 -> 提交。"""
    prev_status = job.status
    if prev_status not in SUBMITTABLE_STATES:
        return {"ok": False,
                "error": f"当前状态 {prev_status} 不可提交（仅 {'/'.join(SUBMITTABLE_STATES)} 可提交）"}
    # CAS：状态原子的 可提交状态 -> UPDATING，防批量+手动/双击并发提交出双作业
    n = (db.query(Job)
         .filter(Job.id == job.id, Job.status.in_(SUBMITTABLE_STATES))
         .update({"status": "UPDATING"}, synchronize_session=False))
    db.commit()
    if n != 1:
        # 会话里的 job.status 是旧值，expire 后重新加载拿真实状态再报
        db.expire(job)
        real = db.get(Job, job.id)
        real_status = real.status if real else "已删除"
        return {"ok": False,
                "error": f"作业状态已变为 {real_status}（可能正在提交/更新中），请刷新后重试"}
    db.refresh(job)

    def _abort(msg: str, **extra) -> dict:
        """提交前拒绝：恢复原始状态（不进入 ERROR）。"""
        job.status = prev_status
        _event(db, job, "submit", msg)
        db.add(job)
        db.commit()
        return {"ok": False, **extra, "error": msg}

    conflict = _find_running_conflict(db, job)
    if conflict:
        return _abort(_conflict_error(conflict))

    # savepoint 恢复前确认旧作业已终态（SeaTunnel 对仍在运行的同 jobId 提交会 no-op 假成功）
    if start_with_savepoint and job.seatunnel_job_id:
        info = st.job_info(db, job.env, job.seatunnel_job_id)
        if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
            final = st.wait_terminal(db, job.env, job.seatunnel_job_id, timeout_sec=30)
            if final not in st.TERMINAL_STATES:
                return _abort(f"旧作业 savepoint 未完成（当前 {final or '未知'}），请稍后重试")

    try:
        ddl_res = doris_ddl.ensure_table(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table, job.field_mapping,
            job.ttl, job.table_model, job.buckets)
        if ddl_res.get("needs_recreate"):
            return _abort(_needs_recreate_msg(ddl_res.get("reasons") or []),
                          needs_recreate=True)
        if ddl_res.get("ttl_altered"):
            _event(db, job, "ddl", "表已存在，已在线更新动态分区配置（TTL）")
        for act in ddl_res.get("online_actions") or []:
            _event(db, job, "ddl", f"在线演进: {act}")
    except Exception as e:  # noqa: BLE001
        _event(db, job, "ddl", f"Doris 建表/加列失败: {e}")
        db.commit()
        return _fail(db, job, "submit", f"Doris 建表/加列失败: {e}")

    version = render.render_and_save(db, job, note="submit")
    version.ddl = ddl_res["ddl"]
    db.add(version)

    params = {}
    if start_with_savepoint and job.seatunnel_job_id:
        params = {"isStartWithSavePoint": "true", "jobId": job.seatunnel_job_id}
    try:
        resp = st.submit_conf(db, job.env, decrypt_safe(job.seatunnel_conf or ""), params)
        job_id = str(resp.get("jobId") or resp.get("job_id") or "")
        if job_id:
            job.seatunnel_job_id = job_id
        job.status = "RUNNING"
        job.status_detail = None
        _event(db, job, "submit", f"提交成功 jobId={job.seatunnel_job_id} version=v{version.version}")
        db.add(job)
        db.commit()
        return {"ok": True, "job_id": job.seatunnel_job_id, "version": version.version, "ddl": ddl_res}
    except Exception as e:  # noqa: BLE001
        return _fail(db, job, "submit", f"提交失败: {e}")


def stop(db: Session, job: Job, with_savepoint: bool = True) -> dict:
    """停止作业（默认带 savepoint）；成功置 STOPPED。失败不改状态，只记事件。"""
    if not job.seatunnel_job_id:
        return {"ok": False, "error": "作业没有 seatunnel_job_id，未提交过？"}
    try:
        # 注意：/stop-job 只接受 JSON 请求体（query 参数会 400）
        st.post(db, job.env, "/stop-job",
              json={"jobId": int(job.seatunnel_job_id), "isStopWithSavePoint": with_savepoint})
        job.status = "STOPPED"
        _event(db, job, "stop", f"isStopWithSavePoint={with_savepoint}")
        db.add(job)
        db.commit()
        return {"ok": True}
    except Exception as e:  # noqa: BLE001
        msg = sanitize_error(f"停止失败: {e}")
        job.status_detail = msg[:2000]
        _event(db, job, "stop", msg)
        db.add(job)
        db.commit()
        return {"ok": False, "error": msg}


def delete(db: Session, job: Job) -> dict:
    """删除作业 = 取消 SeaTunnel 侧作业（如在跑）+ 删除平台记录（单个/批量删除共用）。

    SeaTunnel 没有"删除作业"概念：作业只能 cancel 到终态（FINISHED/CANCELED），
    终态后只是 master 里的历史记录，不占资源。因此删除流程：
    在跑 → /stop-job（不带 savepoint）取消；已终态 → 直接删记录。
    集群不可达时拒绝删除——查不到状态就静默删库会留下孤儿作业继续双写。
    """
    note = ""
    if job.seatunnel_job_id:
        try:
            # 连接级异常：无法确认远端状态，拒绝删除；查不到作业按已终态处理
            info = st.get(db, job.env, f"/job-info/{job.seatunnel_job_id}")
            if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
                st.post(db, job.env, "/stop-job",
                        json={"jobId": int(job.seatunnel_job_id), "isStopWithSavePoint": False})
                note = f"SeaTunnel 侧运行中作业已取消（jobId={job.seatunnel_job_id}）"
            elif info:
                note = "SeaTunnel 侧已是终态（仅历史记录，无需处理）"
        except Exception as e:  # noqa: BLE001
            return {"ok": False,
                    "error": sanitize_error(f"无法确认/取消 SeaTunnel 侧作业（集群不可达），未删除: {e}")}
    # 级联清理指标样本（versions/events 由 ORM cascade 处理，MetricSample 不在 relationship 里）
    db.query(MetricSample).filter(MetricSample.job_id == job.id).delete()
    db.delete(job)
    db.commit()
    return {"ok": True, "note": note}


def update_and_restart(db: Session, job: Job, note: str = "") -> dict:
    """更新编排：防重复检查 -> 表结构预检 -> stop(savepoint) -> 等终态 -> DDL 演进 -> 渲染留档 -> 重启，失败回滚。

    非 RUNNING 状态也可调用（STOPPED/FAILED/ERROR）：跳过停止阶段，STOPPED 带 savepoint 恢复，
    FAILED/ERROR 直接重新提交——「更新并重启」在所有已提交过的状态下语义一致：按新配置生效。
    """
    conflict = _find_running_conflict(db, job)
    if conflict:
        msg = _conflict_error(conflict)
        _event(db, job, "update", msg)
        db.commit()
        return {"ok": False, "stage": "precheck", "error": msg}
    # 表结构预检（停作业之前）：需重建表的冲突直接拒绝，作业保持原状态继续跑
    try:
        pre = doris_ddl.ensure_table(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table, job.field_mapping,
            job.ttl, job.table_model, job.buckets, dry_run=True)
    except Exception as e:  # noqa: BLE001
        msg = f"目标表兼容性预检失败（Doris 不可达？）: {e}"
        _event(db, job, "update", msg)
        db.commit()
        return {"ok": False, "stage": "precheck", "error": msg}
    if pre.get("needs_recreate"):
        msg = _needs_recreate_msg(pre.get("reasons") or [])
        _event(db, job, "update", f"更新被拒绝（作业保持原状态）: {msg}")
        db.commit()
        return {"ok": False, "stage": "precheck", "needs_recreate": True, "error": msg}

    prev_status = job.status
    old_job_id = job.seatunnel_job_id
    # 非 RUNNING 但 SeaTunnel 侧旧作业可能实际还在跑（上次 stop 失败的残留）：
    # 核实后按"在跑"处理，避免跳过停止直接重提交出双作业
    old_running = False
    if old_job_id and prev_status != "RUNNING":
        info = st.job_info(db, job.env, old_job_id)
        old_running = bool(info and str(info.get("jobStatus", "")).upper() == "RUNNING")

    # CAS：原子的 prev -> UPDATING，防与批量/手动并发编排
    n = (db.query(Job)
         .filter(Job.id == job.id, Job.status == prev_status)
         .update({"status": "UPDATING"}, synchronize_session=False))
    db.commit()
    if n != 1:
        # 会话里的 job.status 是旧值，expire 后重新加载拿真实状态再报
        db.expire(job)
        real = db.get(Job, job.id)
        real_status = real.status if real else "已删除"
        return {"ok": False, "stage": "precheck",
                "error": f"作业状态已变为 {real_status}（可能正在提交/更新中），请刷新后重试"}
    db.refresh(job)
    _event(db, job, "update", f"开始更新并重启: {note}")
    db.add(job)
    db.commit()

    if prev_status == "RUNNING" or old_running:
        stop_res = stop(db, job, with_savepoint=True)
        job.status = "UPDATING"  # stop 成功后状态是 STOPPED，编排期间改回 UPDATING
        db.add(job)
        db.commit()
        if not stop_res.get("ok") and old_job_id:
            _event(db, job, "update", f"停止失败，中止更新: {stop_res.get('error')}")
            job.status = "ERROR"
            job.status_detail = stop_res.get("error")
            db.add(job)
            db.commit()
            return {"ok": False, "stage": "stop", "error": stop_res.get("error")}
        if old_job_id:
            final = st.wait_terminal(db, job.env, old_job_id)
            if final not in st.TERMINAL_STATES:
                # 等不到终态继续重提交会被 SeaTunnel 当重复提交 no-op 掉（假成功）
                _event(db, job, "update",
                       f"等待旧作业终态超时（最后状态: {final or '未知'}），中止更新，作业保持运行")
                job.status = "RUNNING"
                db.add(job)
                db.commit()
                return {"ok": False, "stage": "wait_terminal",
                        "error": f"旧作业未在预期时间内停止（{final or '未知'}），已中止更新，作业保持运行"}
            _event(db, job, "update", f"旧作业已终态: {final}")

    try:
        ddl_res = doris_ddl.ensure_table(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table, job.field_mapping,
            job.ttl, job.table_model, job.buckets)
        _event(db, job, "ddl", ddl_res["ddl"])
        if ddl_res.get("needs_recreate"):
            # 预检后表结构被并发改动才可能走到这；拒绝并回滚状态
            msg = _needs_recreate_msg(ddl_res.get("reasons") or [])
            job.status = prev_status
            db.add(job)
            _event(db, job, "update", f"更新被拒绝: {msg}")
            db.commit()
            return {"ok": False, "stage": "ddl", "needs_recreate": True, "error": msg}
        if ddl_res.get("ttl_altered"):
            _event(db, job, "ddl", "表已存在，已在线更新动态分区配置（TTL）")
    except Exception as e:  # noqa: BLE001
        return _fail(db, job, "update", f"字段演进失败: {e}")

    version = render.render_and_save(db, job, note=note or "update")
    version.ddl = ddl_res["ddl"]
    db.add(version)
    db.commit()

    prev = (
        db.query(JobVersion)
        .filter(JobVersion.job_id == job.id, JobVersion.version < version.version)
        .order_by(JobVersion.version.desc())
        .first()
    )
    use_savepoint = bool(old_job_id) and (prev_status in ("RUNNING", "STOPPED") or old_running)
    params = {"isStartWithSavePoint": "true", "jobId": old_job_id} if use_savepoint else {}
    try:
        resp = st.submit_conf(db, job.env, decrypt_safe(job.seatunnel_conf or ""), params)
        job.seatunnel_job_id = str(resp.get("jobId") or resp.get("job_id") or old_job_id or "")
        job.status = "RUNNING"
        job.status_detail = None
        _event(db, job, "update", f"更新完成 version=v{version.version} jobId={job.seatunnel_job_id}")
        db.add(job)
        db.commit()
        return {"ok": True, "job_id": job.seatunnel_job_id, "version": version.version}
    except Exception as e:  # noqa: BLE001
        # 歧义检查：提交"失败"可能只是响应超时，新配置实际已在运行——不能伪造回滚
        info = st.job_info(db, job.env, old_job_id) if old_job_id else None
        if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
            job.status = "RUNNING"
            job.status_detail = (f"更新结果不确定：新配置提交后确认失败，但作业实际在运行"
                                 f"（可能已生效）。请人工核对当前配置: {str(e)[:500]}")
            _event(db, job, "update", "新配置提交结果不确定（疑似已生效），未回滚，请人工核对")
            db.add(job)
            db.commit()
            return {"ok": False, "uncertain": True, "error": str(e)}
        _event(db, job, "update", f"新配置提交失败，尝试回滚: {e}")
        if prev is not None:
            try:
                resp = st.submit_conf(db, job.env, decrypt_safe(prev.conf), params)
                job.seatunnel_conf = prev.conf  # 已是密文存储，原样保留
                # 映射一并回退，否则下次提交会把刚失败的新配置重新渲染应用
                if prev.field_mapping:
                    job.field_mapping = prev.field_mapping
                job.seatunnel_job_id = str(resp.get("jobId") or resp.get("job_id") or old_job_id or "")
                job.status = "RUNNING"
                job.status_detail = f"更新失败已回滚到 v{prev.version}: {e}"
                _event(db, job, "rollback", f"回滚到 v{prev.version} jobId={job.seatunnel_job_id}")
                db.add(job)
                db.commit()
                return {"ok": False, "rolled_back": True, "error": str(e)}
            except Exception as e2:  # noqa: BLE001
                _event(db, job, "rollback", f"回滚失败: {e2}")
        job.status = "ERROR"
        job.status_detail = str(e)[:2000]
        db.add(job)
        db.commit()
        return {"ok": False, "rolled_back": False, "error": str(e)}


def refresh_status(db: Session, job: Job) -> Job:
    """同步 SeaTunnel 侧作业状态；读不到信息不改动，状态变化记事件。

    UPDATING 状态不同步（编排线程自己管理，避免竞争出假状态迁移）。
    """
    if job.status == "UPDATING":
        return job
    if not job.seatunnel_job_id:
        return job
    info = st.job_info(db, job.env, job.seatunnel_job_id)
    if not info:
        return job
    remote = str(info.get("jobStatus", "")).upper()
    new = _STATUS_MAP.get(remote)
    # FAILED 时把 SeaTunnel 的 errorMsg 带出来——per-job 日志依赖 log4j2 配置，
    # 没配置时这是唯一能看到的失败原因
    err = str(info.get("errorMsg") or "").strip()
    if len(err) > 2000:
        # 根因在堆栈末尾的 Caused by 链里：保留头 300 + 尾 1700，中间省略
        err = err[:300] + "\n……（中间省略）……\n" + err[-1700:]
    if new == "FAILED" and err and job.status_detail != err:
        job.status_detail = sanitize_error(err)
        db.add(job)
        db.commit()
    if new and new != job.status:
        old = job.status
        job.status = new
        _event(db, job, "status_change",
               f"{old} -> {new} (SeaTunnel: {remote})" + (f": {err[:500]}" if err else ""))
        db.add(job)
        db.commit()
    return job
