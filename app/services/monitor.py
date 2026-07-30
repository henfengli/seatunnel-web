"""监控数据服务：SeaTunnel/Doris/Kafka 只读查询。

约定：外部调用 5-10s 超时，失败降级（返回 partial + error 字段，不抛异常）；
env 级集群查询带 30 秒内存缓存。
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timedelta

import httpx
from sqlalchemy.orm import Session

from ..models import Job, MetricSample
from ..core.crypto import sanitize_error
from . import doris_ddl, envs, health

_TIMEOUT = 8
_CACHE_TTL = 30
_cache: dict[str, tuple[float, dict]] = {}


def _cached(key: str, fn):
    """30 秒内存缓存（env 级查询用，避免页面刷新反复打集群）。"""
    now = time.time()
    hit = _cache.get(key)
    if hit and now - hit[0] < _CACHE_TTL:
        return hit[1]
    val = fn()
    _cache[key] = (now, val)
    return val


# ---------------------------------------------------------------- 解析工具

def _int(v) -> int:
    try:
        return int(float(str(v)))
    except (TypeError, ValueError):
        return 0


def _pct(v) -> float:
    """百分比字符串 "23.54%" -> 23.54。"""
    m = re.match(r"^\s*([\d.]+)\s*%?\s*$", str(v or ""))
    return float(m.group(1)) if m else 0.0


_SIZE_UNITS = {"B": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}


def _parse_size(v) -> int:
    """容量字符串解析为字节：兼容 "1.5G"、"300M"、"1.234 KB"、裸数字。"""
    m = re.match(r"^\s*([\d.]+)\s*([KMGTPE]?I?B?)\s*$", str(v or "0"), re.IGNORECASE)
    if not m:
        return 0
    unit = (m.group(2) or "B").upper().replace("IB", "").replace("B", "") or "B"
    if unit == "":
        unit = "B"
    return int(float(m.group(1)) * _SIZE_UNITS.get(unit, 1))


def _human(n: float) -> str:
    """字节量人性化展示（与 templating.human_bytes 同风格）。"""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024
    return f"{n:.1f}TB"


# ---------------------------------------------------------------- SeaTunnel REST

def _st_get(env_dict: dict, path: str, raw: bool = False, timeout: int = _TIMEOUT):
    """按顺序尝试各 master，全部失败抛最后一个异常；raw=True 返回文本。"""
    last: Exception | None = None
    for master in env_dict["seatunnel"]["masters"]:
        try:
            resp = httpx.get(master.rstrip("/") + path, timeout=timeout)
            resp.raise_for_status()
            return resp.text if raw else resp.json()
        except Exception as e:  # noqa: BLE001 - 换下一个 master
            last = e
    raise last  # type: ignore[misc]


def seatunnel_cluster(db: Session, env_name: str) -> dict:
    """SeaTunnel 集群概览：/overview + /system-monitoring-information 合并。"""

    def _load() -> dict:
        result = {
            "reachable": False, "version": "", "workers": 0,
            "totalSlot": 0, "unassignedSlot": 0,
            "jobs": {"running": 0, "finished": 0, "failed": 0, "pending": 0, "cancelled": 0},
            "nodes": [], "error": None,
        }
        try:
            env_dict = envs.get_env(db, env_name)
        except KeyError as e:
            result["error"] = str(e)
            return result
        try:
            ov = _st_get(env_dict, "/overview")
            result.update({
                "reachable": True,
                "version": str(ov.get("projectVersion", "")),
                "workers": _int(ov.get("workers")),
                "totalSlot": _int(ov.get("totalSlot")),
                "unassignedSlot": _int(ov.get("unassignedSlot")),
                "jobs": {
                    "running": _int(ov.get("runningJobs")),
                    "finished": _int(ov.get("finishedJobs")),
                    "failed": _int(ov.get("failedJobs")),
                    "pending": _int(ov.get("pendingJobs")),
                    "cancelled": _int(ov.get("cancelledJobs")),
                },
            })
        except Exception as e:  # noqa: BLE001
            result["error"] = sanitize_error(str(e))[:300]
            return result
        try:
            nodes = _st_get(env_dict, "/system-monitoring-information")
            result["nodes"] = [{
                "host": f"{n.get('host', '')}:{n.get('port', '')}",
                "isMaster": str(n.get("isMaster")).lower() == "true",
                "heapUsedPct": _pct(n.get("heap.memory.used/total")),
                "loadProcess": _pct(n.get("load.process")),
                "threadCount": _int(n.get("thread.count")),
                "minorGc": _int(n.get("minor.gc.count")),
                "majorGc": _int(n.get("major.gc.count")),
            } for n in (nodes or [])]
        except Exception as e:  # noqa: BLE001 - 节点信息失败不影响概览
            result["error"] = f"节点信息获取失败: {str(e)[:200]}"
        return result

    return _cached(f"st:{env_name}", _load)


# ---------------------------------------------------------------- Doris 集群

def doris_cluster(db: Session, env_name: str) -> dict:
    """Doris 集群概览：SHOW FRONTENDS/BACKENDS + 库表数量统计。"""

    def _load() -> dict:
        result = {
            "reachable": False,
            "fe": {"total": 0, "alive": 0},
            "be": {"total": 0, "alive": 0, "worstUsedPct": 0.0, "tabletSum": 0, "errNodes": []},
            "dbs": [], "error": None,
        }
        try:
            doris = envs.get_env(db, env_name)["doris"]
        except KeyError as e:
            result["error"] = str(e)
            return result
        try:
            c = doris_ddl._connect(doris)
        except Exception as e:  # noqa: BLE001
            result["error"] = sanitize_error(str(e))[:300]
            return result
        try:
            with c.cursor() as cur:
                cur.execute("SHOW FRONTENDS")
                rows = _rows(cur)
                result["fe"] = {"total": len(rows), "alive": sum(1 for r in rows if _alive(r))}

                cur.execute("SHOW BACKENDS")
                rows = _rows(cur)
                result["be"] = {
                    "total": len(rows),
                    "alive": sum(1 for r in rows if _alive(r)),
                    "worstUsedPct": max((_pct(r.get("MaxDiskUsedPct")) for r in rows), default=0.0),
                    "tabletSum": sum(_int(r.get("TabletNum")) for r in rows),
                    "errNodes": [{"host": r.get("Host", ""), "errMsg": r.get("ErrMsg", "")}
                                 for r in rows if r.get("ErrMsg") or not _alive(r)],
                }

                cur.execute(
                    "SELECT TABLE_SCHEMA, COUNT(*) FROM information_schema.tables "
                    "GROUP BY TABLE_SCHEMA ORDER BY TABLE_SCHEMA"
                )
                result["dbs"] = [
                    {"name": r[0], "tables": r[1]}
                    for r in cur.fetchall() if r[0] not in doris_ddl._SYSTEM_DBS
                ]
            result["reachable"] = True
        except Exception as e:  # noqa: BLE001
            result["error"] = sanitize_error(str(e))[:300]
        finally:
            c.close()
        return result

    return _cached(f"doris:{env_name}", _load)


def _rows(cur) -> list[dict]:
    """cursor 结果转 dict 列表（列名保留原样）。"""
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _alive(row: dict) -> bool:
    return str(row.get("Alive", "")).lower() in ("true", "1")


# ---------------------------------------------------------------- 作业级指标

def _samples(db: Session, job_id: int, since: datetime) -> list[MetricSample]:
    return (
        db.query(MetricSample)
        .filter(MetricSample.job_id == job_id, MetricSample.ts >= since)
        .order_by(MetricSample.ts).all()
    )


def job_metrics_series(db: Session, job: Job, hours: int) -> dict:
    """时间窗内样本由累计计数算区间速率（相邻样本差值/秒）；样本 <2 返回空数组。"""
    samples = _samples(db, job.id, datetime.now() - timedelta(hours=hours))
    out = {"ts": [], "source_qps": [], "sink_qps": [], "source_bps": [],
           "sink_bps": [], "source_total": [], "sink_total": []}
    for prev, cur in zip(samples, samples[1:]):
        dt = (cur.ts - prev.ts).total_seconds() or 1
        out["ts"].append(cur.ts.isoformat(timespec="seconds"))
        out["source_qps"].append(round(max(0, cur.source_count - prev.source_count) / dt, 2))
        out["sink_qps"].append(round(max(0, cur.sink_count - prev.sink_count) / dt, 2))
        out["source_bps"].append(round(max(0, cur.source_bytes - prev.source_bytes) / dt, 1))
        out["sink_bps"].append(round(max(0, cur.sink_bytes - prev.sink_bytes) / dt, 1))
        out["source_total"].append(cur.source_count)
        out["sink_total"].append(cur.sink_count)
    return out


def env_metrics_series(db: Session, env_name: str, hours: int) -> dict:
    """环境级趋势：该环境全部作业（不按当前状态过滤，停止/失败的作业历史保留）
    按作业各自差分（丢弃每个作业窗口内首样本，避免新作业注入假尖刺）后按分钟汇总。"""
    job_ids = [j.id for j in db.query(Job.id).filter(Job.env == env_name).all()]
    out = {"ts": [], "source_qps": [], "sink_qps": []}
    if not job_ids:
        return out
    samples = (
        db.query(MetricSample)
        .filter(MetricSample.job_id.in_(job_ids),
                MetricSample.ts >= datetime.now() - timedelta(hours=hours))
        .order_by(MetricSample.job_id, MetricSample.ts).all()
    )
    by_job: dict[int, list] = {}
    for s in samples:
        by_job.setdefault(s.job_id, []).append(s)
    by_min: dict[datetime, list] = {}
    for job_samples in by_job.values():
        for prev, cur in zip(job_samples, job_samples[1:]):
            dt = (cur.ts - prev.ts).total_seconds() or 1
            key = cur.ts.replace(second=0, microsecond=0)
            agg = by_min.setdefault(key, [0.0, 0.0])
            agg[0] += max(0, cur.source_count - prev.source_count) / dt
            agg[1] += max(0, cur.sink_count - prev.sink_count) / dt
    for key in sorted(by_min):
        out["ts"].append(key.isoformat(timespec="seconds"))
        out["source_qps"].append(round(by_min[key][0], 2))
        out["sink_qps"].append(round(by_min[key][1], 2))
    return out


def job_write_stats(db: Session, job: Job) -> dict:
    """写入统计：1m/1h/1d 速率、累计写入、最后写入时间。"""
    now = datetime.now()

    def _rate(hours: float) -> float:
        samples = _samples(db, job.id, now - timedelta(hours=hours))
        if len(samples) < 2:
            return 0.0
        dt = (samples[-1].ts - samples[0].ts).total_seconds() or 1
        return round(max(0, samples[-1].sink_count - samples[0].sink_count) / dt, 2)

    recent = (
        db.query(MetricSample).filter(MetricSample.job_id == job.id)
        .order_by(MetricSample.ts.desc()).limit(100).all()
    )
    last_write = None
    for cur, prev in zip(recent, recent[1:]):
        if cur.sink_count > prev.sink_count:
            last_write = cur.ts
            break
    # 1m 速率：采集周期本身就是 60s，窗口内通常只有 1 个样本，
    # 直接用最近两个样本差分（作业重启计数归零时差值为负，取 0）
    rate_1m = 0.0
    if len(recent) >= 2:
        dt = (recent[0].ts - recent[1].ts).total_seconds() or 1
        rate_1m = round(max(0, recent[0].sink_count - recent[1].sink_count) / dt, 2)
    return {
        "rate_1m": rate_1m, "rate_1h": _rate(1), "rate_1d": _rate(24),
        "last_write_at": last_write,
        "total_count": recent[0].sink_count if recent else 0,
        "total_bytes": recent[0].sink_bytes if recent else 0,
    }


def kafka_lag(db: Session, job: Job) -> dict | None:
    """kafka 消费滞后（仅 kafka 源且 RUNNING）；group = options.consumer_group or 作业名。

    用 KafkaConsumer.committed() 而不是 KafkaAdminClient.list_consumer_group_offsets——
    后者在部分 kafka-python 版本里不存在。
    """
    if job.source_type != "kafka" or job.status != "RUNNING":
        return None
    try:
        from kafka import KafkaConsumer, TopicPartition

        from .metadata.kafka_d import _admin_kwargs

        conn = health.decrypted(job.datasource.connection)
        group = job.options.get("consumer_group") or job.name
        topic = job.source_ref
        consumer = KafkaConsumer(group_id=group, enable_auto_commit=False,
                                 **_admin_kwargs(conn, request_timeout_ms=5000))
        try:
            tps = [TopicPartition(topic, p)
                   for p in consumer.partitions_for_topic(topic) or set()]
            if not tps:
                return {"error": f"topic {topic} 不存在或无分区"}
            tps.sort(key=lambda t: t.partition)
            ends = consumer.end_offsets(tps)
            committed_map = {tp: consumer.committed(tp) for tp in tps}
        finally:
            consumer.close()
        if all(v in (None, -1) for v in committed_map.values()):
            return {"error": f"消费组 {group} 无 committed offset（可能从未消费或 group 不存在）"}
        parts, total = [], 0
        for tp in tps:
            committed = committed_map.get(tp)
            end = ends.get(tp, 0)
            if committed in (None, -1):
                parts.append({"partition": tp.partition, "end": end,
                              "committed": None, "lag": None})
                continue
            lag = max(0, end - committed)
            total += lag
            parts.append({"partition": tp.partition, "end": end,
                          "committed": committed, "lag": lag})
        return {"partitions": parts, "total_lag": total}
    except Exception as e:  # noqa: BLE001
        return {"error": sanitize_error(str(e))[:300]}


def doris_table_stats(db: Session, job: Job) -> dict:
    """目标表大小（SHOW DATA，解析单位）+ 分区数（SHOW PARTITIONS 计数）。"""
    result = {"size_bytes": 0, "size_human": "-", "partitions": 0, "error": None}
    try:
        doris = envs.get_env(db, job.env)["doris"]
        c = doris_ddl._connect(doris)
        try:
            with c.cursor() as cur:
                cur.execute(f"SHOW DATA FROM `{job.doris_db}`.`{job.doris_table}`")
                total = sum(_parse_size(r.get("Size", 0)) for r in _rows(cur))
                result["size_bytes"] = total
                result["size_human"] = _human(total)
                cur.execute(f"SHOW PARTITIONS FROM `{job.doris_db}`.`{job.doris_table}`")
                result["partitions"] = len(cur.fetchall())
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001
        result["error"] = sanitize_error(str(e))[:300]
    return result


def doris_table_rows(db: Session, job: Job) -> dict:
    """目标表行数（COUNT(*)，大表昂贵，按需调用）。"""
    result = {"rows": None, "error": None}
    try:
        doris = envs.get_env(db, job.env)["doris"]
        c = doris_ddl._connect(doris)
        try:
            with c.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM `{job.doris_db}`.`{job.doris_table}`")
                result["rows"] = int(cur.fetchone()[0])
        finally:
            c.close()
    except Exception as e:  # noqa: BLE001
        result["error"] = sanitize_error(str(e))[:300]
    return result


def checkpoint_stats(db: Session, job: Job) -> dict:
    """checkpoint 统计：/jobs/checkpoints/:jobId（第一个 pipeline）。"""
    result = {"triggered": 0, "completed": 0, "failed": 0,
              "latestDurationMs": None, "stateSize": None, "error": None}
    if not job.seatunnel_job_id:
        result["error"] = "作业未提交过"
        return result
    try:
        data = _st_get(envs.get_env(db, job.env), f"/jobs/checkpoints/{job.seatunnel_job_id}")
        pl = (data.get("pipelines") or [{}])[0]
        counts = pl.get("counts") or {}
        result["triggered"] = _int(counts.get("triggered"))
        result["completed"] = _int(counts.get("completed"))
        result["failed"] = _int(counts.get("failed"))
        latest = pl.get("latestCompleted") or {}
        if latest:
            result["latestDurationMs"] = _int(latest.get("durationMillis"))
            result["stateSize"] = _human(_parse_size(latest.get("stateSize", 0)))
    except Exception as e:  # noqa: BLE001
        result["error"] = sanitize_error(str(e))[:300]
    return result


def _is_engine_log(name: str) -> bool:
    """主日志文件名判断：非 job- 前缀、以 .log 结尾（排除 seatunnel.log.2026-07-24-1 这类滚动归档）。"""
    base = name.rsplit("/", 1)[-1]
    return base.endswith(".log") and not base.startswith("job-")


def engine_logs(db: Session, env_name: str, tail: int = 500) -> dict:
    """环境级引擎主日志：第一个可达 master 的主日志截尾（routing 分流后作业日志不在这里）。"""
    result = {"lines": [], "link": "", "error": None}
    try:
        env_dict = envs.get_env(db, env_name)
        all_logs = _st_get(env_dict, "/logs?format=json") or []
        engine = next((e for e in all_logs
                       if _is_engine_log(e.get("logName") or "")), None)
        if not engine:
            result["error"] = "未找到引擎主日志（SeaTunnel 版本/部署可能不支持 /logs 接口）"
            return result
        link = engine.get("logLink") or engine.get("logName") or ""
        result["link"] = link
        text = (httpx.get(link, timeout=10).text if link.startswith("http")
                else _st_get(env_dict, f"/logs/{link}", raw=True, timeout=15))
        result["lines"] = text.splitlines()[-tail:]
    except Exception as e:  # noqa: BLE001
        result["error"] = sanitize_error(str(e))[:300]
    return result


def job_logs(db: Session, job: Job, tail: int = 500) -> dict:
    """作业日志：优先 job-<jobId>.log；没有则兜底拉引擎主日志按 jobId 过滤。

    注：per-job 日志文件依赖 SeaTunnel log4j2 的 job appender 配置，未配置时只有引擎主日志。
    数据相关的转换失败（写入 N 行后炸）根因堆栈常在截尾窗口之前，
    截尾内没有 ERROR 时自动把全文的 ERROR/Caused by 块提到最前展示。
    """
    result = {"lines": [], "link": "", "error": None}
    if not job.seatunnel_job_id:
        result["error"] = "作业未提交过，无日志"
        return result

    def _pack(text: str) -> list[str]:
        lines = text.splitlines()
        tail_lines = lines[-tail:]
        if any("ERROR" in l for l in tail_lines):
            return tail_lines
        hits = [l for l in lines if "ERROR" in l or "Caused by" in l or "Exception in thread" in l]
        if hits:
            return hits[-120:] + ["", "……（以下为日志截尾）……", ""] + tail_lines
        return tail_lines

    try:
        env_dict = envs.get_env(db, job.env)
        entries = _st_get(env_dict, f"/logs/{job.seatunnel_job_id}?format=json")
        if entries:
            link = entries[0].get("logLink") or entries[0].get("logName") or ""
            result["link"] = link
            text = (httpx.get(link, timeout=10).text if link.startswith("http")
                    else _st_get(env_dict, f"/logs/{link}", raw=True, timeout=15))
            result["lines"] = _pack(text)
            return result
        # 兜底：引擎主日志按 jobId 过滤
        all_logs = _st_get(env_dict, "/logs?format=json") or []
        engine = next((e for e in all_logs
                       if _is_engine_log(e.get("logName") or "")), None)
        if not engine:
            result["error"] = "SeaTunnel 侧没有该作业的日志文件，且未找到引擎主日志"
            return result
        link = engine.get("logLink") or engine.get("logName") or ""
        result["link"] = link
        text = (httpx.get(link, timeout=10).text if link.startswith("http")
                else _st_get(env_dict, f"/logs/{link}", raw=True, timeout=15))
        hits = [l for l in text.splitlines() if job.seatunnel_job_id in l]
        if hits:
            result["lines"] = hits[-tail:]
            result["note"] = "未配置 per-job 日志，以下为引擎主日志中该作业的条目"
        else:
            result["error"] = ("SeaTunnel 未生成 per-job 日志（log4j2 未配置 job appender），"
                               "引擎主日志中也没有该作业的条目")
    except Exception as e:  # noqa: BLE001
        result["error"] = sanitize_error(str(e))[:300]
    return result
