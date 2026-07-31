# -*- coding: utf-8 -*-
"""批量功能端到端测试：批量建作业全流程 + 批量操作（启动/改配置/停止/删除，mock SeaTunnel REST）。"""
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from app.models import BatchTask, Datasource, Environment, Job, ProtoPackage
from app.services import doris_ddl, proto_center
from app.services.field_mapping import append_timestamp_columns, build_mapping

from .helpers import check

# ---------------------------------------------------------------- mock SeaTunnel（批量操作用）
ST = {"status": "RUNNING", "stops": 0, "submits": 0}


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
            ST["status"] = "RUNNING"
            ST["submits"] += 1
            self._json({"jobId": q.get("jobId", ["733584788375666689"])[0], "jobName": "mock"})
        elif u.path == "/stop-job":
            if not body:
                self._json({"error": "Request body is empty."}, 400)
                return
            payload = json.loads(body)
            ST["stops"] += 1
            ST["status"] = "FINISHED" if payload.get("isStopWithSavePoint") else "CANCELED"
            self._json({"jobId": str(payload.get("jobId", "")), "jobName": "mock"})
        else:
            self._json({"error": "not found"}, 404)

    def do_GET(self):
        if self.path.startswith("/job-info"):
            self._json({"jobId": "733584788375666689", "jobName": "mock",
                        "jobStatus": ST["status"], "createTime": 1, "metrics": {}})
        else:
            self._json({"error": "not found"}, 404)

    def log_message(self, *a):
        pass


@pytest.fixture(scope="module", autouse=True)
def mock_services():
    """mock SeaTunnel REST + 绕过 Doris DDL（模块级，测完还原）。"""
    server = HTTPServer(("127.0.0.1", 18081), MockSeaTunnel)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    orig = doris_ddl.ensure_table
    doris_ddl.ensure_table = lambda *a, **kw: {"created": True, "added_columns": [],
                                               "ddl": "-- mock ddl"}
    yield
    doris_ddl.ensure_table = orig
    server.shutdown()


PROTO = """
syntax = "proto3";
message Sub { string a = 1; }
message Tick { int64 ts = 1; double px = 2; Sub nest = 3; }
"""


def _pack(pairs):
    """list-of-tuples -> dict-of-lists（重复表单字段）。"""
    d: dict[str, list] = {}
    for k, v in pairs:
        d.setdefault(k, []).append(v)
    return d


def map_fields(p, mapping, flags=None):
    flags = flags or {}
    t = []
    for m in mapping:
        t += [
            (f"{p}map_enabled", "1"),
            (f"{p}map_source", m["source"]),
            (f"{p}map_nested", "1" if m["nested"] else "0"),
            (f"{p}map_note", m.get("note", "")),
            (f"{p}map_sink_only", "1" if m.get("sink_only") else "0"),
            (f"{p}map_ms_epoch", "1" if m.get("ms_epoch") else "0"),
            (f"{p}map_default", m.get("default", "")),
            (f"{p}map_src_path", m.get("src_path", "")),
            (f"{p}map_src_root", m.get("src_root", "")),
            (f"{p}map_src_root_type", m.get("src_root_type", "")),
            (f"{p}map_st_type", m["st_type"]),
            (f"{p}map_doris_col", m["doris_col"]),
            (f"{p}map_doris_type", m["doris_type"]),
            (f"{p}map_flags", flags.get(m["source"], "")),
            (f"{p}map_agg", ""),
        ]
    return t


def test_batch_e2e(db, client):
    def wait_task_done(task_id: int, timeout: float = 30) -> BatchTask:
        """轮询批量任务直到 DONE（后台线程执行）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            db.expire_all()
            task = db.get(BatchTask, task_id)
            if task and task.status == "DONE":
                return task
            time.sleep(0.3)
        raise TimeoutError(f"批量任务 #{task_id} 未在 {timeout}s 内完成")

    env = db.query(Environment).filter_by(name="dev").first()
    if env is None:
        env = Environment(name="dev", doris_fenodes="127.0.0.1:8030", doris_query_port=9030,
                          doris_username="root", doris_password="", variant_enabled=True)
    env.seatunnel_masters = "http://127.0.0.1:18081"
    db.add(env)
    ds = Datasource(
        env="dev", name="bk", type="kafka", connection={"servers": "k:9092"},
        metadata_status="ok",
        metadata_dict={
            "topics": ["derivatives.view.ipp", "derivatives.view.pos", "trade.order"],
            "consumer_groups": [],
        })
    pkg = ProtoPackage(name="btick", content=PROTO,
                       parsed=proto_center.parse_proto(PROTO),
                       status="current", current_version="v1")
    db.add_all([ds, pkg])
    db.commit()

    # 1. step1 页面
    r = client.get("/jobs/batch-new")
    check("step1 页面 200", r.status_code == 200 and "批量新建作业" in r.text)

    # 2. 数据源下拉（batch 模式联动多选）
    r = client.get("/api/datasources/options", params={"batch": "1", "env": "dev", "type": "kafka"})
    check("ds 下拉联动 batch-objects", "/api/datasources/batch-objects" in r.text)

    # 3. 对象多选列表
    r = client.get("/api/datasources/batch-objects", params={"datasource_id": str(ds.id)})
    check("对象多选 3 个", r.text.count('name="objects"') == 3, str(r.text.count('name="objects"')))

    shared = [("env", "dev"), ("source_type", "kafka"), ("datasource_id", str(ds.id)),
              ("proto_package_id", str(pkg.id)), ("message_name", "Tick"),
              ("tags", "batch"), ("doris_db", "btest"), ("add_timestamps", "on"),
              ("parallelism", "2"), ("checkpoint_interval", ""), ("buckets", ""), ("start_mode", "")]

    # 4. preview：逐对象配置页（重复字段用 dict-of-list 传，本版 TestClient 不认 list-of-tuples）
    r = client.post("/jobs/batch-new/preview",
                    data={**dict(shared),
                          "objects": ["derivatives.view.ipp", "derivatives.view.pos"]})
    check("preview 200", r.status_code == 200, str(r.status_code))
    check("默认作业名 IPP/POS（kafka 末段大写）", 'value="IPP"' in r.text and 'value="POS"' in r.text)
    check("默认目标表 kafka_ipp", 'value="kafka_ipp"' in r.text)
    check("两个对象块", r.text.count('name="o_idx"') == 2)
    check("映射含 kafka_ts", 'name="o0_map_source" value="kafka_ts"' in r.text)
    check("TTL 下拉存在", 'name="o0_ttl_column"' in r.text)

    # 5. create：o0 带 TTL(epoch 毫秒列)，o1 UNIQUE+key，o2 TTL 缺时间字段（应失败）
    mapping = build_mapping("kafka", proto_center.flattened_schema_fields(pkg, "Tick", set()), True)
    append_timestamp_columns(mapping, "kafka")

    data = list(shared)
    data += [("o_idx", "0"), ("o_idx", "1"), ("o_idx", "2")]
    data += [("o0_ref", "derivatives.view.ipp"), ("o0_name", "IPP"),
             ("o0_doris_table", "kafka_ipp"), ("o0_table_model", "DUPLICATE"),
             ("o0_ttl_num", "30"), ("o0_ttl_unit", "DAY"), ("o0_ttl_column", "ts")]
    data += map_fields("o0_", mapping)
    data += [("o1_ref", "derivatives.view.pos"), ("o1_name", "POS"),
             ("o1_doris_table", "kafka_pos"), ("o1_table_model", "UNIQUE"),
             ("o1_ttl_num", ""), ("o1_ttl_unit", "DAY"), ("o1_ttl_column", "")]
    data += map_fields("o1_", mapping, flags={"ts": "key"})
    data += [("o2_ref", "trade.order"), ("o2_name", "ORDER"),
             ("o2_doris_table", "kafka_order"), ("o2_table_model", "DUPLICATE"),
             ("o2_ttl_num", "10"), ("o2_ttl_unit", "DAY"), ("o2_ttl_column", "")]
    data += map_fields("o2_", mapping)

    r = client.post("/jobs/batch-create", data=_pack(data))
    check("create 200", r.status_code == 200, str(r.status_code))
    check("成功 2/3（o2 应失败）", "成功 2 / 3" in r.text)
    check("o2 报 TTL 缺时间字段", "请选择 TTL 时间字段" in r.text)

    db.expire_all()
    j0 = db.query(Job).filter_by(name="IPP").first()
    check("IPP 已创建 DRAFT", j0 is not None and j0.status == "DRAFT")
    check("IPP TTL 落 options", j0 and j0.options.get("ttl_num") == 30
          and j0.options.get("ttl_column") == "ts", str(j0 and j0.options))
    ts_col = next((m for m in (j0.field_mapping if j0 else []) if m["doris_col"] == "ts"), {})
    check("TTL epoch 列转 DATETIMEV2(3)", ts_col.get("ms_epoch") is True
          and ts_col["doris_type"] == "DATETIMEV2(3)", str(ts_col))
    check("IPP conf 已预渲染", bool(j0 and j0.seatunnel_conf))
    check("IPP 共享 parallelism", j0 and j0.options.get("parallelism") == 2, str(j0 and j0.options))

    j1 = db.query(Job).filter_by(name="POS").first()
    check("POS UNIQUE 模型", j1 and j1.options.get("table_model") == "UNIQUE", str(j1 and j1.options))
    check("POS key 标记在 ts", any(m.get("is_key") for m in (j1.field_mapping if j1 else [])
                                   if m["source"] == "ts"))
    check("ORDER 未创建", db.query(Job).filter_by(name="ORDER").first() is None)

    # 6. 重复批次：同名/同源拦截
    r = client.post("/jobs/batch-create", data=_pack(data))
    check("重复批次被拦截", "已存在同名作业" in r.text and "成功 0 / 3" in r.text)

    # 7. 拍平展开重渲染（batch-mapping 片段）
    r = client.get("/api/jobs/batch-mapping", params={
        "p": "o0_", "o0_ds": str(ds.id), "o0_stype": "kafka", "o0_ref": "trade.order",
        "o0_env": "dev", "o0_pkg": str(pkg.id), "o0_msg": "Tick", "o0_addts": "on",
        "o0_flatten_nest": "1",
    })
    check("拍平展开 nest_a", 'name="o0_map_src_path" value="nest.a"' in r.text, str(r.status_code))

    # ---------------------------------------------------------------- 批量操作
    db.expire_all()
    j0 = db.query(Job).filter_by(name="IPP").first()
    j1 = db.query(Job).filter_by(name="POS").first()

    # 8. 空勾选拦截
    r = client.post("/jobs/batch", data={"action": "start"})
    check("空勾选被拦截", "请先勾选作业" in r.text)

    # 9. 批量启动（DRAFT -> RUNNING）
    r = client.post("/jobs/batch", data={"action": "start", "job_ids": [str(j0.id), str(j1.id)]})
    check("批量启动跳转进度页", r.status_code == 200 and "批量任务" in r.text, str(r.status_code))
    task = db.query(BatchTask).order_by(BatchTask.id.desc()).first()
    task = wait_task_done(task.id)
    check("批量启动全部 OK", task.ok_count == 2, f"{task.ok_count}/{task.total}")
    db.expire_all()
    check("两作业 RUNNING", db.get(Job, j0.id).status == "RUNNING"
          and db.get(Job, j1.id).status == "RUNNING")

    # 10. 批量改配置（parallelism + 标签 + restart）
    r = client.post("/jobs/batch", data={
        "action": "options", "job_ids": [str(j0.id), str(j1.id)],
        "parallelism": "8", "checkpoint_interval": "", "fetch_max_bytes": "",
        "max_poll_records": "", "buckets": "", "start_mode": "", "consumer_group": "",
        "tags": "batch,new", "restart": "on",
    })
    task = wait_task_done(db.query(BatchTask).order_by(BatchTask.id.desc()).first().id)
    check("批量改配置全部 OK", task.ok_count == 2, f"{task.ok_count}/{task.total}")
    db.expire_all()
    j0 = db.get(Job, j0.id)
    check("options 已 merge", j0.options.get("parallelism") == 8
          and j0.options.get("ttl_num") == 30, str(j0.options))
    check("标签已替换", j0.tags == "batch,new", j0.tags)
    check("restart 后仍 RUNNING", j0.status == "RUNNING", j0.status)

    # 11. 批量停止
    r = client.post("/jobs/batch", data={"action": "stop", "job_ids": [str(j0.id), str(j1.id)]})
    task = wait_task_done(db.query(BatchTask).order_by(BatchTask.id.desc()).first().id)
    check("批量停止全部 OK", task.ok_count == 2, f"{task.ok_count}/{task.total}")
    db.expire_all()
    check("两作业 STOPPED", db.get(Job, j0.id).status == "STOPPED"
          and db.get(Job, j1.id).status == "STOPPED")

    # 12. 状态守卫：STOPPED 再停止 -> 全部 SKIPPED
    r = client.post("/jobs/batch", data={"action": "stop", "job_ids": [str(j0.id), str(j1.id)]})
    task = wait_task_done(db.query(BatchTask).order_by(BatchTask.id.desc()).first().id)
    check("重复停止全部 SKIPPED", task.ok_count == 0
          and all(i.status == "SKIPPED" for i in task.items),
          str([(i.job_name, i.status) for i in task.items]))

    # 13. 批量删除（STOPPED -> SeaTunnel 已终态 -> 删记录）
    j0_id, j1_id = j0.id, j1.id  # 删除后对象失效，先取 id
    r = client.post("/jobs/batch", data={"action": "delete", "job_ids": [str(j0_id), str(j1_id)]})
    task = wait_task_done(db.query(BatchTask).order_by(BatchTask.id.desc()).first().id)
    check("批量删除全部 OK", task.ok_count == 2, f"{task.ok_count}/{task.total}")
    db.expire_all()
    check("两作业已删除", db.get(Job, j0_id) is None and db.get(Job, j1_id) is None)

    # 14. 进度页展示
    r = client.get(f"/batch/{task.id}")
    check("进度页 DONE 展示", r.status_code == 200 and "DONE" in r.text
          and "setTimeout" not in r.text)
    r = client.get(f"/batch/{task.id}/progress")
    check("进度片段 200 且 DONE 不再轮询", r.status_code == 200
          and "hx-get" not in r.text, str(r.status_code))
