# -*- coding: utf-8 -*-
"""编排器端到端测试：mock SeaTunnel REST + 绕过 Doris DDL，验证 submit/stop/update 全流程。"""
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, ".")

from app.core.db import SessionLocal, init_db  # noqa: E402
from app.models import Datasource, Environment, Job, ProtoPackage  # noqa: E402
from app.services import orchestrator, doris_ddl, proto_center  # noqa: E402

STATE = {"job_id": None, "status": "RUNNING", "stops": 0, "submits": [], "conf_bodies": []}


class MockSeaTunnel(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace")
        if u.path == "/submit-job":
            STATE["job_id"] = q.get("jobId", ["733584788375666689"])[0]
            STATE["status"] = "RUNNING"
            STATE["submits"].append(dict(q))
            STATE["conf_bodies"].append(body)
            self._json({"jobId": STATE["job_id"], "jobName": "mock"})
        elif u.path == "/stop-job":
            # 与真实 SeaTunnel 一致：只接受 JSON 请求体，空 body 返回 400
            if not body:
                self._json({"error": "Request body is empty."}, 400)
                return
            payload = json.loads(body)
            STATE["stops"] += 1
            STATE["status"] = "FINISHED" if payload.get("isStopWithSavePoint") else "CANCELED"
            self._json({"jobId": str(payload.get("jobId", "")), "jobName": "mock"})
        else:
            self._json({"error": "not found"}, 404)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path.startswith("/job-info"):
            # 2.3.13 真实结构：指标内嵌在 metrics 字段，值全是字符串
            self._json({"jobId": STATE["job_id"], "jobName": "mock",
                        "jobStatus": STATE["status"], "createTime": 1,
                        "metrics": {
                            "SourceReceivedCount": "1000",
                            "SourceReceivedQPS": "100.5",
                            "SinkWriteCount": "990",
                            "SinkWriteQPS": "99.5",
                            "SourceReceivedBytes": "2048000",
                            "SinkWriteBytes": "2040000",
                        }})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, *a):
        pass


server = HTTPServer(("127.0.0.1", 18080), MockSeaTunnel)
threading.Thread(target=server.serve_forever, daemon=True).start()

# 绕过 Doris DDL（环境在 DB 中，masters 指向 mock 端口，见下方 dev 环境插入）
doris_ddl.ensure_table = lambda *a, **kw: {"created": True, "added_columns": [], "ddl": "-- mock ddl"}

PROTO = 'syntax = "proto3"; message Tick { int64 ts = 1; double px = 2; }'

ok = True


def check(name, cond, extra=""):
    global ok
    print(("PASS" if cond else "FAIL"), name, extra)
    if not cond:
        ok = False


init_db()
db = SessionLocal()
# 幂等：清掉上次运行的种子残留（Job 及其 versions/events/metrics；SQLite id 复用会挂上旧版本）
from app.models import JobVersion, JobEvent, MetricSample  # noqa: E402

_old_ids = [r[0] for r in db.query(Job.id).filter(Job.name.in_(["orch_test", "orch_test_dup"])).all()]
db.query(JobVersion).filter(JobVersion.job_id.in_(_old_ids or [-1])).delete(synchronize_session=False)
db.query(JobEvent).filter(JobEvent.job_id.in_(_old_ids or [-1])).delete(synchronize_session=False)
db.query(MetricSample).filter(MetricSample.job_id.in_(_old_ids or [-1])).delete(synchronize_session=False)
db.query(Job).filter(Job.name.in_(["orch_test", "orch_test_dup"])).delete(synchronize_session=False)
db.query(Datasource).filter_by(env="dev", name="k").delete(synchronize_session=False)
db.query(ProtoPackage).filter_by(name="tick").delete(synchronize_session=False)
db.commit()
# 环境已改为 DB 管理：dev 环境 masters 必须指向上面的 mock SeaTunnel（upsert）
dev_env = db.query(Environment).filter_by(name="dev").first()
if dev_env is None:
    dev_env = Environment(name="dev", doris_fenodes="127.0.0.1:8030", doris_query_port=9030,
                          doris_username="root", doris_password="",
                          variant_enabled=True, default_buckets=10)
dev_env.seatunnel_masters = "http://127.0.0.1:18080"
db.add(dev_env)
db.commit()
pkg = ProtoPackage(name="tick", content=PROTO,
                   parsed_json=ProtoPackage._dumps(proto_center.parse_proto(PROTO)),
                   status="current", current_version="v1")
ds = Datasource(env="dev", name="k", type="kafka", connection_json='{"servers": "k:9092"}',
                metadata_status="ok")
db.add_all([pkg, ds])
db.commit()

mapping = [
    {"source": "ts", "st_type": "bigint", "doris_col": "ts", "doris_type": "BIGINT", "nested": False},
    {"source": "px", "st_type": "double", "doris_col": "px", "doris_type": "DOUBLE", "nested": False},
]
job = Job(name="orch_test", env="dev", biz_line="md", source_type="kafka", datasource_id=ds.id,
          source_ref="ticks", doris_db="seatunnel_sync", doris_table="md_kafka_ticks",
          proto_package_id=pkg.id, message_name="Tick",
          field_mapping_json=Job._dumps(mapping), options_json="{}")
db.add(job)
db.commit()

# 1. submit
r = orchestrator.submit(db, job)
check("submit ok", r.get("ok") is True, str(r))
check("job RUNNING + jobId", job.status == "RUNNING" and job.seatunnel_job_id == "733584788375666689",
      f"{job.status}/{job.seatunnel_job_id}")
from app.core.crypto import decrypt_safe  # noqa: E402

check("conf 留档 v1", len(job.versions) == 1
      and "format = protobuf" in decrypt_safe(job.versions[0].conf))

# 2. refresh_status + collect_metrics
orchestrator.refresh_status(db, job)
check("refresh RUNNING", job.status == "RUNNING", job.status)
orchestrator.collect_metrics(db, job)
from app.models import MetricSample  # noqa: E402
m = db.query(MetricSample).filter_by(job_id=job.id).first()
check("metrics 落库", m is not None and m.source_count == 1000 and m.sink_qps == 99.5,
      str(m and (m.source_count, m.sink_qps)))

# 3. update_and_restart：stop(savepoint) → submit(带 savepoint+jobId) → v2 留档
r = orchestrator.update_and_restart(db, job, note="proto 更新")
check("update_and_restart ok", r.get("ok") is True, str(r))
check("stop 被调用 1 次", STATE["stops"] == 1, str(STATE["stops"]))
last_submit = STATE["submits"][-1]
check("带 savepoint 重启", last_submit.get("isStartWithSavePoint") == ["true"]
      and last_submit.get("jobId") == ["733584788375666689"], str(last_submit))
db.expire(job)  # 会话 expire_on_commit=False，清掉缓存的 versions 集合再计数
check("版本留档 v2", len(job.versions) == 2)
check("事件时间线", db.query(type(job.events[0])).filter_by(job_id=job.id).count() >= 4)

# 3b. 防双作业重复消费：同 env+数据源+source_ref 的 RUNNING 作业存在时拒绝提交
job2 = Job(name="orch_test_dup", env="dev", biz_line="md", source_type="kafka", datasource_id=ds.id,
           source_ref="ticks", doris_db="seatunnel_sync", doris_table="md_kafka_ticks2",
           proto_package_id=pkg.id, message_name="Tick",
           field_mapping_json=Job._dumps(mapping), options_json="{}")
db.add(job2)
db.commit()
r = orchestrator.submit(db, job2)
check("重复消费被拦截", r.get("ok") is False and "写双份" in r.get("error", ""), str(r))
check("被拦截后仍为 DRAFT", job2.status == "DRAFT", job2.status)

# 4. stop
r = orchestrator.stop(db, job, with_savepoint=True)
check("stop ok", r.get("ok") is True and job.status == "STOPPED", f"{r}/{job.status}")

# 4b. 第一个作业停止后，第二个可以正常提交
r = orchestrator.submit(db, job2)
check("停止后可正常提交", r.get("ok") is True and job2.status == "RUNNING", f"{r}/{job2.status}")

db.close()
server.shutdown()
print("ORCH", "OK" if ok else "FAILED")
sys.exit(0 if ok else 1)
