"""SQLAlchemy 模型 —— 对应方案设计 §2 核心概念模型。

JSON 文本列统一用 JsonDict（core/db.py）：Python 侧直接读写 dict/list，
数据库列名保持 *_json 不变，已有库无需迁移。
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .core.db import Base, JsonDict

DS_TYPES = ("kafka", "mongodb", "postgresql", "doris")

JOB_STATUSES = (
    "DRAFT",      # 已配置未提交
    "RUNNING",    # 在 SeaTunnel 上运行中
    "STOPPED",    # 已停止（带 savepoint）
    "FAILED",     # SeaTunnel 侧失败
    "UPDATING",   # 正在走"更新并重启"编排
    "ERROR",      # 管理端操作失败
)


def _now() -> datetime:
    return datetime.now()


class Environment(Base):
    """逻辑环境：SeaTunnel master 列表 + Doris 连接 + 可选 proto 站点（Web 可维护）。

    环境表为空时由 environments.yaml 一次性种子导入，之后全部在 Web 上管理。
    注意：Datasource.env / Job.env 是对 name 的弱引用（非外键），
    改名/删除必须走 Web 层的引用检查，不要直接操作 DB。
    """
    __tablename__ = "environments"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True, index=True)  # 字母/数字/下划线/中划线
    seatunnel_masters: Mapped[str] = mapped_column(Text, default="")   # 原始文本：每行一个或逗号分隔
    doris_fenodes: Mapped[str] = mapped_column(String(256), default="")
    doris_query_port: Mapped[int] = mapped_column(Integer, default=9030)
    doris_username: Mapped[str] = mapped_column(String(64), default="root")
    doris_password: Mapped[str] = mapped_column(Text, default="")      # 加密存储
    variant_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    default_buckets: Mapped[int] = mapped_column(Integer, default=10)
    replication_num: Mapped[int] = mapped_column(Integer, default=1)  # 单机 Doris 为 1；生产集群手动改 3
    proto_site_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    proto_site_auth: Mapped[str | None] = mapped_column(Text, nullable=True)  # 加密存储
    health: Mapped[dict | None] = mapped_column("health_json", JsonDict(dict), nullable=True)  # 最近连接测试结果（seatunnel/doris + checked_at）
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)


class Datasource(Base):
    __tablename__ = "datasources"
    __table_args__ = (UniqueConstraint("env", "name", name="uq_ds_env_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    env: Mapped[str] = mapped_column(String(32), index=True)
    name: Mapped[str] = mapped_column(String(128))
    type: Mapped[str] = mapped_column(String(16))          # kafka/mongodb/postgresql/doris
    connection: Mapped[dict] = mapped_column("connection_json", JsonDict(dict), default=dict)  # 密码字段已加密
    metadata_dict: Mapped[dict] = mapped_column("metadata_json", JsonDict(dict), nullable=True)  # 元数据缓存（topics/库表字段）
    metadata_status: Mapped[str] = mapped_column(String(16), default="pending")  # ok/expired/error/pending
    metadata_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    health_status: Mapped[str] = mapped_column(String(8), default="unknown")  # ok/fail/unknown
    health_detail: Mapped[str | None] = mapped_column(Text, nullable=True)    # 最近连接测试结果
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    jobs: Mapped[list["Job"]] = relationship(back_populates="datasource")


class ProtoPackage(Base):
    __tablename__ = "proto_packages"
    __table_args__ = (UniqueConstraint("name", name="uq_proto_name"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128))
    origin: Mapped[str] = mapped_column(String(16), default="url")  # url=站点拉取 / paste=手动粘贴 / upload=文件上传
    source_url: Mapped[str] = mapped_column(Text, default="")
    auth_header: Mapped[str] = mapped_column(Text, default="")   # 加密存储
    poll_interval_sec: Mapped[int] = mapped_column(Integer, default=3600)
    current_version: Mapped[str | None] = mapped_column(String(64), nullable=True)  # etag/版本号
    content: Mapped[str | None] = mapped_column(Text, nullable=True)                # 当前 .proto 原文
    parsed: Mapped[dict] = mapped_column("parsed_json", JsonDict(dict), nullable=True)      # 解析产物（字段树）
    prev_content: Mapped[str | None] = mapped_column(Text, nullable=True)           # 上一版（用于回滚/diff）
    prev_parsed: Mapped[dict] = mapped_column("prev_parsed_json", JsonDict(dict), nullable=True)
    diff: Mapped[dict] = mapped_column("diff_json", JsonDict(dict), nullable=True)          # 最近一次的 diff
    status: Mapped[str] = mapped_column(String(16), default="pending")  # current/updated/error/pending
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    jobs: Mapped[list["Job"]] = relationship(back_populates="proto_package")

    @property
    def top_level_messages(self) -> list[str]:
        """parsed 结构: {"messages": {msg_name: field_tree}, "top_level": [names]}"""
        return (self.parsed or {}).get("top_level", [])


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    env: Mapped[str] = mapped_column(String(32), index=True)
    biz_line: Mapped[str] = mapped_column(String(64), default="default")  # 业务线（进 Doris 命名）
    tags: Mapped[str] = mapped_column(String(256), default="")            # 逗号分隔
    source_type: Mapped[str] = mapped_column(String(16))                  # kafka/mongodb/postgresql/doris
    datasource_id: Mapped[int] = mapped_column(ForeignKey("datasources.id"))
    source_ref: Mapped[str] = mapped_column(String(256))                  # topic | db.table | db.collection
    doris_db: Mapped[str] = mapped_column(String(128))
    doris_table: Mapped[str] = mapped_column(String(128))
    proto_package_id: Mapped[int | None] = mapped_column(ForeignKey("proto_packages.id"), nullable=True)
    message_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # [{source, st_type, doris_col, doris_type, nested, ...}]
    field_mapping: Mapped[list] = mapped_column("field_mapping_json", JsonDict(list), nullable=True)
    seatunnel_conf: Mapped[str | None] = mapped_column(Text, nullable=True)      # 当前渲染产物
    seatunnel_job_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(16), default="DRAFT", index=True)
    status_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    options: Mapped[dict] = mapped_column("options_json", JsonDict(dict), default=dict)  # parallelism/checkpoint/批大小等
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_now, onupdate=_now)

    datasource: Mapped[Datasource] = relationship(back_populates="jobs")
    proto_package: Mapped[ProtoPackage | None] = relationship(back_populates="jobs")
    versions: Mapped[list["JobVersion"]] = relationship(
        back_populates="job", order_by="JobVersion.version.desc()", cascade="all, delete-orphan"
    )
    events: Mapped[list["JobEvent"]] = relationship(
        back_populates="job", order_by="JobEvent.created_at.desc()", cascade="all, delete-orphan"
    )

    @property
    def tag_list(self) -> list[str]:
        return [t.strip() for t in self.tags.split(",") if t.strip()]


class JobVersion(Base):
    """配置即代码：每次提交的 conf 快照全留档。"""
    __tablename__ = "job_versions"
    __table_args__ = (UniqueConstraint("job_id", "version", name="uq_job_ver"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    version: Mapped[int] = mapped_column(Integer)
    conf: Mapped[str] = mapped_column(Text)
    field_mapping: Mapped[list] = mapped_column("field_mapping_json", JsonDict(list), nullable=True)
    ddl: Mapped[str | None] = mapped_column(Text, nullable=True)     # 本次执行的建表/加列语句
    proto_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    note: Mapped[str] = mapped_column(String(256), default="")
    created_by: Mapped[str] = mapped_column(String(64), default="system")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    job: Mapped[Job] = relationship(back_populates="versions")


class JobEvent(Base):
    __tablename__ = "job_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"))
    event: Mapped[str] = mapped_column(String(64))    # submit/stop/update/status_change/alert/ddl...
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)

    job: Mapped[Job] = relationship(back_populates="events")


class MetricSample(Base):
    __tablename__ = "metric_samples"

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id"), index=True)
    source_count: Mapped[int] = mapped_column(default=0)
    sink_count: Mapped[int] = mapped_column(default=0)
    source_qps: Mapped[float] = mapped_column(default=0)
    sink_qps: Mapped[float] = mapped_column(default=0)
    source_bytes: Mapped[int] = mapped_column(default=0)
    sink_bytes: Mapped[int] = mapped_column(default=0)
    ts: Mapped[datetime] = mapped_column(DateTime, default=_now, index=True)


class BatchTask(Base):
    """批量操作任务：后台线程串行执行，页面轮询进度。"""
    __tablename__ = "batch_tasks"

    id: Mapped[int] = mapped_column(primary_key=True)
    action: Mapped[str] = mapped_column(String(16))       # start/stop/restart/delete/options
    status: Mapped[str] = mapped_column(String(16), default="PENDING", index=True)  # PENDING/RUNNING/DONE
    total: Mapped[int] = mapped_column(Integer, default=0)
    done: Mapped[int] = mapped_column(Integer, default=0)
    ok_count: Mapped[int] = mapped_column(Integer, default=0)
    params: Mapped[dict] = mapped_column("params_json", JsonDict(dict), default=dict)  # options 批改字段/标签/是否重启
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_now)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    items: Mapped[list["BatchItem"]] = relationship(
        back_populates="task", order_by="BatchItem.id", cascade="all, delete-orphan"
    )


class BatchItem(Base):
    """批量任务逐条结果：status = PENDING/RUNNING/OK/SKIPPED/FAILED。

    job_id 是弱引用（非外键）：作业删除后保留原 id 仅作历史记录。
    """
    __tablename__ = "batch_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("batch_tasks.id"), index=True)
    job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    job_name: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16), default="PENDING")
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped[BatchTask] = relationship(back_populates="items")
