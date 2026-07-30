"""页面路由：返回 HTML（Jinja2 渲染），表单提交与服务层编排操作。

约定：
- 普通表单提交 -> 303 重定向；htmx 请求 -> HX-Redirect 头（见 templating.goto）。
- 服务层返回 {"ok": False, "error": ...} 时，错误通过 query 参数 flash 到目标页。
- 密码等敏感字段只写不读：编辑表单留空表示不修改。
- 按领域拆分子模块，这里只做汇总。
"""
from fastapi import APIRouter

from . import batch, dashboard, datasource, env, job, proto

router = APIRouter()
# batch 要在 job 之前：/jobs/batch-new 否则会被 /jobs/{job_id} 抢先匹配成 422
for _r in (dashboard.router, env.router, datasource.router,
           proto.router, batch.router, job.router):
    router.include_router(_r)
