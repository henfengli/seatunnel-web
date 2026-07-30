# -*- coding: utf-8 -*-
"""全页面渲染验证：种子数据后逐个 GET 主要路由，断言 200 且无渲染异常。

运行: .venv/Scripts/python.exe pages_test.py（在 seatunnel-web/ 目录下）
"""
import sys

sys.path.insert(0, ".")

from fastapi.testclient import TestClient  # noqa: E402

from app.core.db import SessionLocal, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app.models import (BatchItem, BatchTask, Datasource, Environment,  # noqa: E402
                        Job, JobEvent, ProtoPackage)
from app.services import proto_center  # noqa: E402

ok = True


def check(name, cond, extra=""):
    global ok
    print(("PASS" if cond else "FAIL"), name, extra)
    if not cond:
        ok = False


PROTO = 'syntax = "proto3"; message Tick { int64 ts = 1; double px = 2; }'

init_db()
db = SessionLocal()
# 幂等种子
db.query(Job).filter(Job.name == "page_test").delete(synchronize_session=False)
db.query(Datasource).filter_by(env="dev", name="pk").delete(synchronize_session=False)
db.query(ProtoPackage).filter_by(name="ptick").delete(synchronize_session=False)
db.query(BatchItem).delete(synchronize_session=False)
db.query(BatchTask).delete(synchronize_session=False)
env = db.query(Environment).filter_by(name="dev").first()
if env is None:
    env = Environment(name="dev", seatunnel_masters="http://127.0.0.1:18082",
                      doris_fenodes="127.0.0.1:8030", doris_query_port=9030,
                      doris_username="root", doris_password="", variant_enabled=True)
    db.add(env)
db.commit()
ds = Datasource(env="dev", name="pk", type="kafka", connection_json='{"servers": "k:9092"}',
                metadata_status="ok",
                metadata_json=Datasource._dumps({"topics": ["a.b.c"], "consumer_groups": ["g1"]}))
pkg = ProtoPackage(name="ptick", content=PROTO,
                   parsed_json=ProtoPackage._dumps(proto_center.parse_proto(PROTO)),
                   status="current", current_version="v1")
db.add_all([ds, pkg])
db.commit()
job = Job(name="page_test", env="dev", biz_line="db1", tags="t1,t2", source_type="kafka",
          datasource_id=ds.id, source_ref="a.b.c", doris_db="db1", doris_table="kafka_c",
          proto_package_id=pkg.id, message_name="Tick",
          field_mapping_json='[{"source":"ts","st_type":"bigint","doris_col":"ts","doris_type":"BIGINT","nested":false}]',
          options_json="{}", status="STOPPED", seatunnel_job_id="123")
db.add(job)
db.commit()
db.add(JobEvent(job_id=job.id, event="submit", detail="提交失败: mock 平台侧错误"))
db.commit()
task = BatchTask(action="stop", status="DONE", total=1, done=1, ok_count=1)
db.add(task)
db.flush()
db.add(BatchItem(batch_id=task.id, job_id=job.id, job_name=job.name, status="OK"))
db.commit()

client = TestClient(app)

ROUTES = [
    "/",
    "/environments", "/environments/new", f"/environments/{env.id}/edit",
    f"/environments/{env.id}/logs",
    "/datasources", "/datasources/new", f"/datasources/{ds.id}", f"/datasources/{ds.id}/edit",
    "/protos", "/protos/new", f"/protos/{pkg.id}", f"/protos/{pkg.id}/edit",
    "/jobs", "/jobs?env=dev&status=STOPPED&tag=t1", "/jobs/new", "/jobs/batch-new",
    f"/jobs/{job.id}", f"/jobs/{job.id}/edit", f"/jobs/{job.id}/recreate-table",
    "/monitor", "/monitor?env=dev",
    f"/batch/{task.id}",
    # htmx 片段
    f"/jobs/{job.id}/monitor", f"/jobs/{job.id}/badge",
    "/api/datasources/options?env=dev&type=kafka",
    "/api/datasources/batch-objects?datasource_id=" + str(ds.id),
    f"/api/datasources/{ds.id}/objects",
    f"/api/protos/{pkg.id}/messages",
    f"/api/jobs/{job.id}/logs",
]

for url in ROUTES:
    try:
        r = client.get(url)
        check(f"GET {url}", r.status_code == 200 and "Traceback" not in r.text,
              str(r.status_code))
    except Exception as e:  # noqa: BLE001
        check(f"GET {url}", False, f"{type(e).__name__}: {e}")

r = client.get(f"/api/jobs/{job.id}/logs")
check("日志面板含平台操作日志", "平台操作日志" in r.text and "mock 平台侧错误" in r.text)

# 大表单回归：80 个映射字段（80x14+杂项 > Starlette 默认 1000 域上限）创建作业不被拒
def _pack(pairs):
    """list-of-tuples -> dict-of-lists（本版 TestClient 重复表单字段只认这种编码）。"""
    d: dict[str, list] = {}
    for k, v in pairs:
        d.setdefault(k, []).append(v)
    return d


big = [("name", "big_form_job"), ("env", "dev"), ("source_type", "kafka"),
       ("datasource_id", str(ds.id)), ("source_ref", "a.b.c"),
       ("proto_package_id", str(pkg.id)), ("message_name", "Tick"),
       ("tags", ""), ("doris_db", "db1"), ("doris_table", "big_t"), ("add_timestamps", "")]
for i in range(80):
    big += [("map_enabled", "1"), ("map_source", f"f{i}"), ("map_st_type", "bigint"),
            ("map_doris_col", f"f{i}"), ("map_doris_type", "BIGINT"), ("map_nested", "0"),
            ("map_note", ""), ("map_sink_only", "0"), ("map_default", ""),
            ("map_src_path", ""), ("map_src_root", ""), ("map_src_root_type", ""),
            ("map_flags", ""), ("map_agg", "")]
r = client.post("/jobs", data=_pack(big))
check("大表单创建（>1000 域）", r.status_code == 200, str(r.status_code))
db.expire_all()
_big = db.query(Job).filter_by(name="big_form_job").first()
check("大表单作业落库 80 字段", _big is not None and len(_big.field_mapping) == 80,
      str(_big and len(_big.field_mapping)))
db.query(Job).filter(Job.name == "big_form_job").delete(synchronize_session=False)
db.commit()

# 清理
db.query(Job).filter(Job.name == "page_test").delete(synchronize_session=False)
db.query(Datasource).filter_by(env="dev", name="pk").delete(synchronize_session=False)
db.query(ProtoPackage).filter_by(name="ptick").delete(synchronize_session=False)
db.query(BatchItem).delete(synchronize_session=False)
db.query(BatchTask).delete(synchronize_session=False)
db.commit()
db.close()
print("PAGES", "OK" if ok else "FAILED")
sys.exit(0 if ok else 1)
