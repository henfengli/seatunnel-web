"""提交/停止/更新编排：SeaTunnel Zeta REST 调用 + Doris DDL + 版本留档 + 事件记录。

统一风格：公开编排函数返回 {"ok": bool, ...} dict，不抛异常；错误落 Job.status/JobEvent。
"""
from __future__ import annotations

import time

import httpx
from sqlalchemy.orm import Session

from ..core.crypto import decrypt_safe, sanitize_error
from ..models import Job, JobEvent, JobVersion, MetricSample
from . import doris_ddl, envs, render

_TIMEOUT = 10
_TERMINAL_STATES = ("FINISHED", "CANCELED", "FAILED")
# SeaTunnel 作业状态 -> 管理端状态（其余状态不改动）
_STATUS_MAP = {"RUNNING": "RUNNING", "FAILED": "FAILED", "FINISHED": "STOPPED", "CANCELED": "STOPPED"}


def _request(db: Session, env: str, method: str, path: str, retry: bool = True, **kw) -> dict:
    """按顺序尝试各 master，全部失败抛最后一个异常；返回 JSON。

    HTTP 错误时把 SeaTunnel 返回的错误正文附在异常里（否则 500 只剩状态码，无法排查）。
    retry=False 只试第一个 master——非幂等请求（submit-job）跨 master 重试可能产生重复作业。
    """
    last: Exception | None = None
    masters = envs.get_env(db, env)["seatunnel"]["masters"]
    for master in (masters if retry else masters[:1]):
        url = master.rstrip("/") + path
        try:
            with httpx.Client(timeout=_TIMEOUT) as client:
                resp = client.request(method, url, **kw)
                if resp.status_code >= 400:
                    body = resp.text[:500].strip()
                    raise RuntimeError(f"SeaTunnel 返回 {resp.status_code}: {body}")
                return resp.json() if resp.content else {}
        except Exception as e:  # noqa: BLE001 - 换下一个 master 重试
            last = e
    raise last  # type: ignore[misc]


def _post(db: Session, env: str, path: str, **kw) -> dict:
    return _request(db, env, "POST", path, **kw)


def _get(db: Session, env: str, path: str, **kw) -> dict:
    return _request(db, env, "GET", path, **kw)


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


def _submit_conf(db: Session, env: str, conf: str, extra_params: dict | None = None) -> dict:
    """提交 HOCON 配置到 /submit-job（不跨 master 重试：非幂等，重试可能重复提交）。"""
    params = {"format": "hocon", **(extra_params or {})}
    return _request(db, env, "POST", "/submit-job", retry=False, params=params, content=conf,
                    headers={"Content-Type": "text/plain; charset=utf-8"})


def _job_info(db: Session, env: str, job_id: str) -> dict | None:
    """GET /job-info/{id}，失败回退 /job-info?jobId=；都失败返回 None。"""
    try:
        return _get(db, env, f"/job-info/{job_id}")
    except Exception:  # noqa: BLE001
        try:
            return _get(db, env, "/job-info", params={"jobId": job_id})
        except Exception:  # noqa: BLE001
            return None


def _job_ttl(job: Job) -> dict | None:
    """从作业高级选项取 TTL 配置 {"num","unit","column"}；兼容老数据 options["ttl_days"]（按 DAY）。"""
    opts = job.options
    column = opts.get("ttl_column")
    num = opts.get("ttl_num") or opts.get("ttl_days")
    if num and column:
        ttl = {"num": int(num), "unit": opts.get("ttl_unit", "DAY"), "column": column}
        if opts.get("ttl_history_num"):
            ttl["history_num"] = int(opts["ttl_history_num"])
        return ttl
    return None


def _job_buckets(job: Job) -> int | None:
    """从作业高级选项取分桶数覆盖，未配置返回 None（用环境默认值）。"""
    v = job.options.get("buckets")
    return int(v) if v else None


def _job_model(job: Job) -> str:
    """目标 Doris 表模型（默认 DUPLICATE；options["table_model"] 仅在 UNIQUE 时存储）。"""
    return job.options.get("table_model", "DUPLICATE")


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


def submit(db: Session, job: Job, start_with_savepoint: bool = False) -> dict:
    """CAS 防并发提交 -> 防重复消费 -> savepoint 就绪检查 -> 建表 -> 渲染留档 -> 提交 SeaTunnel。"""
    prev_status = job.status
    # CAS：状态原子的 DRAFT/FAILED/ERROR/STOPPED -> UPDATING，防批量+手动/双击并发提交出双作业
    n = (db.query(Job)
         .filter(Job.id == job.id, Job.status == prev_status)
         .update({"status": "UPDATING"}, synchronize_session=False))
    db.commit()
    if n != 1:
        return {"ok": False,
                "error": f"作业状态已变为 {job.status}（可能正在提交/更新中），请刷新后重试"}
    db.expire(job)
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
        info = _job_info(db, job.env, job.seatunnel_job_id)
        if info and str(info.get("jobStatus", "")).upper() == "RUNNING":
            final = _wait_terminal(db, job.env, job.seatunnel_job_id, timeout_sec=30)
            if final not in _TERMINAL_STATES:
                return _abort(f"旧作业 savepoint 未完成（当前 {final or '未知'}），请稍后重试")

    try:
        ddl_res = doris_ddl.ensure_table(
            envs.get_env(db, job.env)["doris"], job.doris_db, job.doris_table, job.field_mapping,
            _job_ttl(job), _job_model(job), _job_buckets(job))
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
        resp = _submit_conf(db, job.env, decrypt_safe(job.seatunnel_conf or ""), params)
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
        _post(db, job.env, "/stop-job",
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


def _wait_terminal(db: Session, env: str, job_id: str, timeout_sec: int = 60, interval_sec: int = 2) -> str:
    """轮询 job-info 直到终态或超时，返回最后看到的 SeaTunnel 状态。"""
    deadline = time.time() + timeout_sec
    status = ""
    while time.time() < deadline:
        info = _job_info(db, env, job_id)
        status = str((info or {}).get("jobStatus", "")).upper()
        if status in _TERMINAL_STATES:
            break
        time.sleep(interval_sec)
    return status


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
            _job_ttl(job), _job_model(job), _job_buckets(job), dry_run=True)
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
        info = _job_info(db, job.env, old_job_id)
        old_running = bool(info and str(info.get("jobStatus", "")).upper() == "RUNNING")

    # CAS：原子的 prev -> UPDATING，防与批量/手动并发编排
    n = (db.query(Job)
         .filter(Job.id == job.id, Job.status == prev_status)
         .update({"status": "UPDATING"}, synchronize_session=False))
    db.commit()
    if n != 1:
        return {"ok": False, "stage": "precheck",
                "error": f"作业状态已变为 {job.status}（可能正在提交/更新中），请刷新后重试"}
    db.expire(job)
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
            final = _wait_terminal(db, job.env, old_job_id)
            if final not in _TERMINAL_STATES:
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
            _job_ttl(job), _job_model(job), _job_buckets(job))
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
        resp = _submit_conf(db, job.env, decrypt_safe(job.seatunnel_conf or ""), params)
        job.seatunnel_job_id = str(resp.get("jobId") or resp.get("job_id") or old_job_id or "")
        job.status = "RUNNING"
        job.status_detail = None
        _event(db, job, "update", f"更新完成 version=v{version.version} jobId={job.seatunnel_job_id}")
        db.add(job)
        db.commit()
        return {"ok": True, "job_id": job.seatunnel_job_id, "version": version.version}
    except Exception as e:  # noqa: BLE001
        # 歧义检查：提交"失败"可能只是响应超时，新配置实际已在运行——不能伪造回滚
        info = _job_info(db, job.env, old_job_id) if old_job_id else None
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
                resp = _submit_conf(db, job.env, decrypt_safe(prev.conf), params)
                job.seatunnel_conf = prev.conf  # 已是密文存储，原样保留
                # 映射一并回退，否则下次提交会把刚失败的新配置重新渲染应用
                if prev.field_mapping_json:
                    job.field_mapping_json = prev.field_mapping_json
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
    info = _job_info(db, job.env, job.seatunnel_job_id)
    if not info:
        return job
    st = str(info.get("jobStatus", "")).upper()
    new = _STATUS_MAP.get(st)
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
               f"{old} -> {new} (SeaTunnel: {st})" + (f": {err[:500]}" if err else ""))
        db.add(job)
        db.commit()
    return job


def _num(v) -> float | None:
    """数值解析：2.3.13 的 metrics 值全是字符串，兼容 int/float/数字字符串。"""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        try:
            return float(v) if "." in v else int(v)
        except ValueError:
            return None
    return None


def _flatten_metrics(obj, out: dict) -> None:
    """防御性展开 metrics JSON：数值项按名字累加，支持 {name, value} 列表结构与字符串值。"""
    if isinstance(obj, dict):
        if "name" in obj and "value" in obj and _num(obj.get("value")) is not None:
            out[str(obj["name"])] = out.get(str(obj["name"]), 0) + _num(obj["value"])
            return
        for k, v in obj.items():
            n = _num(v) if isinstance(k, str) else None
            if n is not None:
                out[k] = out.get(k, 0) + n
            else:
                _flatten_metrics(v, out)
    elif isinstance(obj, list):
        for item in obj:
            _flatten_metrics(item, out)


def _pick(flat: dict, *keys: str):
    """先精确匹配，再子串匹配。"""
    for key in keys:
        if key in flat:
            return flat[key]
    for key in keys:
        for k, v in flat.items():
            if key in k:
                return v
    return 0


def collect_metrics(db: Session, job: Job) -> MetricSample | None:
    """采集 source/sink 计数、QPS、字节数写 MetricSample；取不到返回 None。

    注意：2.3.13 没有 /job-metrics 端点（旧实现一直 404，容量面板全 0），
    指标内嵌在 GET /job-info/:jobId 的 metrics 字段，且值全是字符串。
    """
    if not job.seatunnel_job_id:
        return None
    info = _job_info(db, job.env, job.seatunnel_job_id)
    if not info:
        return None
    flat: dict = {}
    _flatten_metrics(info.get("metrics") or {}, flat)
    if not flat:
        return None
    sample = MetricSample(
        job_id=job.id,
        source_count=int(_pick(flat, "TableSourceReceivedCount", "SourceReceivedCount")),
        sink_count=int(_pick(flat, "TableSinkWriteCount", "SinkWriteCount")),
        source_qps=float(_pick(flat, "TableSourceReceivedQPS", "SourceReceivedQPS")),
        sink_qps=float(_pick(flat, "TableSinkWriteQPS", "SinkWriteQPS")),
        source_bytes=int(_pick(flat, "TableSourceReceivedBytes", "SourceReceivedBytes")),
        sink_bytes=int(_pick(flat, "TableSinkWriteBytes", "SinkWriteBytes")),
    )
    db.add(sample)
    db.commit()
    return sample
