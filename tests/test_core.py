# -*- coding: utf-8 -*-
"""平台级小件回归：环境种子一次性标记 / flash 消息截断 / 字节格式化。"""
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

from starlette.requests import Request

from app.core.fmt import human_bytes
from app.models import Environment
from app.services import envs
from app.templating import goto

from .helpers import check

_SEED_YAML = {
    "seeded": {
        "seatunnel": {"masters": ["http://127.0.0.1:9999"]},
        "doris": {"fenodes": "127.0.0.1:8030", "query_port": 9030,
                  "username": "root", "password": ""},
    }
}


def test_seed_only_once(db, tmp_path, monkeypatch):
    """种子只导一次：标记文件判定，删光环境后重启不复活；老库升级只补标记。"""
    monkeypatch.setattr(envs, "DATA_DIR", tmp_path)
    settings = SimpleNamespace(environments=_SEED_YAML)

    check("空库首次导入", envs.seed_from_yaml(db, settings) == 1)
    check("第二次被标记挡住", envs.seed_from_yaml(db, settings) == 0)

    db.query(Environment).filter_by(name="seeded").delete()
    db.commit()
    check("删光环境后不复活", envs.seed_from_yaml(db, settings) == 0
          and db.query(Environment).filter_by(name="seeded").first() is None)

    # 老库升级路径：无标记但已有环境 -> 只补标记不重复导入
    (tmp_path / ".env_seeded").unlink()
    db.add(Environment(name="existing", seatunnel_masters="http://x:1",
                       doris_fenodes="x:8030", doris_query_port=9030,
                       doris_username="root", doris_password=""))
    db.commit()
    check("老库只补标记", envs.seed_from_yaml(db, settings) == 0
          and (tmp_path / ".env_seeded").exists()
          and db.query(Environment).filter_by(name="seeded").first() is None)


def test_goto_truncates_flash():
    """flash 消息超 300 字截断：query 参数/HX-Redirect 头容不下 Doris 长错误。"""
    scope = {"type": "http", "method": "GET", "path": "/", "headers": []}
    r = goto(Request(scope), "/jobs", "错" * 500, ok=False)
    check("303 跳转", r.status_code == 303)
    msg = parse_qs(urlparse(r.headers["location"]).query)["msg"][0]
    check("消息被截断", len(msg) == 301 and msg.endswith("…"), str(len(msg)))

    r = goto(Request(scope), "/jobs", "正常消息")
    msg = parse_qs(urlparse(r.headers["location"]).query)["msg"][0]
    check("短消息原样", msg == "正常消息")


def test_human_bytes():
    check("格式化", human_bytes(2048000) == "2.0MB" and human_bytes(512) == "512B")
    check("非法输入降级", human_bytes("abc") == "-" and human_bytes(None) == "0B")


def test_json_dict_mutable_tracking(db):
    """JsonDict + MutableDict/MutableList：顶层键/元素原地改必须落库（嵌套变更仍需整体重赋值）。"""
    from app.models import Job, Datasource, Environment

    env = db.query(Environment).filter_by(name="mut").first()
    if env is None:
        env = Environment(name="mut", doris_fenodes="f:8030")
        db.add(env)
    ds = db.query(Datasource).filter_by(env="mut", name="mut").first()
    if ds is None:
        ds = Datasource(env="mut", name="mut", type="kafka", connection={"servers": "k:9092"})
        db.add(ds)
    db.commit()
    job = Job(name="mut_track", env="mut", biz_line="b", source_type="kafka",
              datasource_id=ds.id, source_ref="t", doris_db="d", doris_table="t",
              field_mapping=[{"source": "a"}], options={"parallelism": 1})
    db.add(job)
    db.commit()
    jid = job.id

    job.options["parallelism"] = 4          # 顶层键原地改
    job.field_mapping.append({"source": "b"})  # list 元素原地加
    db.commit()
    db.expire_all()
    fresh = db.get(Job, jid)
    check("dict 顶层原地改落库", fresh.options["parallelism"] == 4, str(fresh.options))
    check("list 原地 append 落库", len(fresh.field_mapping) == 2, str(fresh.field_mapping))
