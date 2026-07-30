# Vision-SeaTunnel 管理端

轻量级 SeaTunnel 作业管理平台：统一管理 Kafka / MongoDB / PostgreSQL / Doris → Doris 的同步作业。

- 后端：FastAPI + SQLAlchemy（SQLite）+ APScheduler
- 前端：Jinja2 服务端渲染 + htmx（本地静态文件，无 Node 构建链、无 CDN 依赖，可直接用于内网）
- 与 SeaTunnel 集群只通过 REST API 交互，本系统重启/升级不影响在跑作业

## 功能

- **环境管理（Web 可创建）**：每个环境 = SeaTunnel master 地址列表 + Doris 连接 + proto 站点（可选）。
  `environments.yaml` 里的环境段仅首次启动时作为种子导入，之后全部在 Web 上维护
- **连接测试**：新建/编辑环境与数据源时可"测试连接"，失败也允许保存；列表页用绿/红/灰圆点标识健康状态
- **数据源管理**：连接信息加密存储；Kafka 支持 SASL/SSL 认证（PLAIN/SCRAM）与自定义 kafka.config；
  Kafka topic / MongoDB 库集合字段 / PG 库表字段 / Doris 库表 自动发现
- **Proto 中心**：从 proto 站点定时拉取或手动粘贴，自动解析（顶层 message 识别、字段树、版本 diff、schema 漂移标记）
- **作业向导**：选数据源 → 选源表/topic → 选 message → 字段映射自动生成（源类型 → SeaTunnel → Doris）→ 目标表按命名规范自动生成、不存在自动建表、新字段自动加列。
  MongoDB 源可选**批式快照**（一次性全量，job.mode=BATCH）或 **CDC 持续同步**（change streams，job.mode=STREAMING，startup.mode 可选 initial/latest，需 Mongo 副本集；UNIQUE 模型自动开 delete sign）
- **提交编排**：提交 / 带 savepoint 停止 / 更新并重启（停→DDL 演进→渲染→断点续传→失败自动回滚）
- **配置即代码**：每次渲染的 conf、字段映射、DDL、proto 版本全量留档，可回溯
- **环境晋升**：作业定义环境无关，一键复制到其他环境（自动换绑集群与数据源）
- **批量建作业**：一个数据源多选源对象（topic/表），逐对象配置作业名/目标表/表模型/TTL/字段映射，一次建出一批 DRAFT
- **批量操作**：作业列表勾选（支持先按环境/标签/状态过滤再全选）→ 批量启动/停止/更新并重启/删除、批量改高级选项与标签；
  后台任务串行执行，进度页逐条结果，失败聚合一条钉钉告警
- **容量面板**：按环境/业务线聚合 QPS 与字节量；状态看护 + 异常告警（钉钉机器人 webhook）

## 钉钉告警配置

作业 RUNNING→FAILED、proto schema 漂移、批量操作部分失败时会推送钉钉消息。配置在 `environments.yaml` 的 `watchdog` 段：

```yaml
watchdog:
  # 钉钉群 → 群机器人 → 添加「自定义」机器人，复制 webhook 地址
  alert_webhook: "https://oapi.dingtalk.com/robot/send?access_token=你的token"
  # 安全设置二选一：
  #  1) 加签：把 SEC 开头的密钥填到 alert_secret（推荐，不受关键词限制）
  #  2) 自定义关键词：alert_secret 留空，关键词设为 SeaTunnel 或 告警（告警标题固定含「SeaTunnel 平台告警」）
  alert_secret: ""
```

留空 `alert_webhook` 则关闭告警（只记日志）。

## 部署

```bash
# 1. 解压
tar -xzf seatunnel-web.tar.gz && cd seatunnel-web

# 2. 安装依赖（Python 3.10+，内网走公司 PyPI 源即可）
pip install -r requirements.txt

# 3. 启动（环境在 Web 上创建，无需先改配置文件）
python -m app.main
# 或 uvicorn app.main:app --host 0.0.0.0 --port 8000
```

访问 `http://localhost:8000`，先到「环境」页创建你的第一套环境。

## 数据与备份

运行数据全部在 `data/` 目录（vision.db + secret.key），**备份 = 拷目录**。
注意 `secret.key` 是密码加密密钥，丢失则已存的数据源/环境密码无法解密。

## 验证

测试用 pytest 组织在 `tests/` 下（不依赖外部服务，SeaTunnel/Doris 均为 mock，
数据目录隔离在临时目录，不会碰 `data/`）：

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

- `test_smoke.py`：proto 解析 / 字段映射 / conf 渲染管线 / SASL 模板输出 / DDL 生成
- `test_orch.py`：提交编排全流程（mock SeaTunnel REST）
- `test_batch.py`：批量建作业 + 批量操作（路由级，mock SeaTunnel REST）
- `test_recreate.py`：表结构兼容检查 / 数据迁移重建 / 提交更新预检（模拟 Doris + mock SeaTunnel）
- `test_pages.py`：全页面路由渲染验证（30 个 GET 路由）

## 目录结构

```
app/
├── api/
│   ├── pages/        # 页面路由（按领域拆 dashboard/env/datasource/proto/job/batch）
│   └── fragments.py  # htmx 联动片段 + 监控 JSON（/api 前缀）
├── core/             # 全局配置 / SQLite / 加密
├── services/         # 环境 / proto中心 / 字段映射 / 渲染 / Doris DDL / 元数据发现
│                     # / 提交编排 / 状态看护 / 连接测试 / SeaTunnel HTTP 客户端 / 告警
├── templates/
│   ├── conf/         # 各源类型 HOCON Jinja2 模板
│   └── pages/        # 页面模板
├── static/           # htmx + 样式（全部本地化）
├── models.py
└── main.py
tests/                # pytest 测试（见上）
environments.yaml     # 全局配置（命名规范/看护间隔/告警webhook；环境段仅为首启种子）
data/                 # 运行数据（SQLite / 密钥，首次启动自动生成）
```

## 前置要求

- SeaTunnel Zeta 集群已开启 REST API（`seatunnel.yaml` 的 `engine.http`，默认 8080）
- 若使用 Kafka protobuf 嵌套结构（`array<{...}>`），SeaTunnel 需打入 protobuf 补丁
  （见项目根目录 `scripts/patch-protobuf-fix.sh` 与 `protobuf解析修复-审核后.md`）
- Doris 账号需有建库建表权限（自动建表/加列用）
