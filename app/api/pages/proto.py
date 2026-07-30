"""Proto 包中心：站点拉取 / 手动粘贴 / 文件上传 + 轮询 + diff。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, File, Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.concurrency import run_in_threadpool

from ...core.crypto import encrypt
from ...core.db import get_db
from ...models import Job, ProtoPackage
from ...services import proto_center
from ...templating import goto, templates
from .common import _NAME_RE, _form_dict, form_error

router = APIRouter()


def _strip_proto_suffix(name: str) -> str:
    """用户输入的名称若带 .proto 后缀则去掉（页面上已暗示无需手动输入）。"""
    return name[:-6] if name.lower().endswith(".proto") else name


# ---------------------------------------------------------------- Proto 包

@router.get("/protos", response_class=HTMLResponse)
def proto_list(request: Request, db: Session = Depends(get_db)):
    pkgs = db.query(ProtoPackage).order_by(ProtoPackage.id).all()
    return templates.TemplateResponse(request, "protos.html", {
        "active": "protos", "pkgs": pkgs,
    })


@router.get("/protos/new", response_class=HTMLResponse)
def proto_new(request: Request):
    return templates.TemplateResponse(request, "proto_form.html", {
        "active": "protos", "pkg": None, "error": None, "form": {},
    })


@router.post("/protos")
async def proto_create(request: Request, db: Session = Depends(get_db)):
    """创建 proto 包：粘贴内容走 update_content，否则填了 source_url 走首次拉取。"""
    form = await request.form()

    def _err(msg: str):
        return form_error(request, "proto_form.html", msg,
                          active="protos", pkg=None, form=_form_dict(form))

    name = _strip_proto_suffix((form.get("name") or "").strip())
    source_url = (form.get("source_url") or "").strip()
    content = (form.get("content") or "").strip()
    auth_header = (form.get("auth_header") or "").strip()
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    if not source_url and not content:
        return _err("请填写来源 URL 或直接粘贴 proto 内容")
    try:
        poll_interval = max(60, int(form.get("poll_interval_sec") or 3600))
    except ValueError:
        return _err("拉取间隔必须是数字（秒）")

    pkg = ProtoPackage(
        name=name, source_url=source_url, poll_interval_sec=poll_interval,
        auth_header=encrypt(auth_header) if auth_header else "",
        origin="paste" if content else "url",
    )
    db.add(pkg)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名 proto 包: {name}")

    # 首次载入内容：优先粘贴内容，其次 URL 拉取
    if content:
        await run_in_threadpool(proto_center.update_content, db, pkg, content)
    else:
        await run_in_threadpool(proto_center.poll_package, db, pkg)
    if pkg.status == "error":
        return goto(request, f"/protos/{pkg.id}",
                    f"proto 包已创建，但首次载入失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg.id}", f"proto 包 {name} 已创建")


@router.post("/protos/upload")
async def proto_upload(request: Request, db: Session = Depends(get_db),
                       files: list[UploadFile] = File(...)):
    """批量上传 .proto 文件：每个文件建一个 proto 包（名称取文件名去后缀）。"""
    created, skipped, failed = [], [], []
    for f in files:
        name = _strip_proto_suffix((f.filename or "").rsplit("/", 1)[-1].strip())
        if not name or not _NAME_RE.match(name):
            failed.append(f"{f.filename}（文件名不合法）")
            continue
        if db.query(ProtoPackage).filter(ProtoPackage.name == name).first():
            skipped.append(name)
            continue
        try:
            content = (await f.read()).decode("utf-8")
        except UnicodeDecodeError:
            failed.append(f"{f.filename}（非 UTF-8 文本）")
            continue
        pkg = ProtoPackage(name=name, source_url="", origin="upload")
        db.add(pkg)
        db.commit()
        await run_in_threadpool(proto_center.update_content, db, pkg, content)
        if pkg.status == "error":
            failed.append(f"{name}（解析失败: {pkg.error}）")
        else:
            created.append(name)
    parts = []
    if created:
        parts.append(f"成功 {len(created)} 个: {', '.join(created)}")
    if skipped:
        parts.append(f"跳过同名 {len(skipped)} 个: {', '.join(skipped)}")
    if failed:
        parts.append(f"失败 {len(failed)} 个: {', '.join(failed)}")
    return goto(request, "/protos", "；".join(parts) or "未选择文件",
                ok=not (failed and not created))


@router.get("/protos/{pkg_id}", response_class=HTMLResponse)
def proto_detail(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    diff = pkg.diff or None
    parsed = pkg.parsed or {}
    return templates.TemplateResponse(request, "proto_detail.html", {
        "active": "protos", "pkg": pkg, "diff": diff,
        "messages": parsed.get("messages", {}),
        "top_level": parsed.get("top_level", []),
    })


@router.get("/protos/{pkg_id}/edit", response_class=HTMLResponse)
def proto_edit(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    return templates.TemplateResponse(request, "proto_form.html", {
        "active": "protos", "pkg": pkg, "error": None, "form": {},
    })


@router.post("/protos/{pkg_id}")
async def proto_update(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """更新 proto 包基本信息；auth_header 留空不修改；粘贴内容则重新解析。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    form = await request.form()

    def _err(msg: str):
        return form_error(request, "proto_form.html", msg,
                          active="protos", pkg=pkg, form={})

    name = _strip_proto_suffix((form.get("name") or "").strip())
    if not _NAME_RE.match(name):
        return _err("名称必填，仅限字母/数字/_.-，最长 128 字符")
    try:
        poll_interval = max(60, int(form.get("poll_interval_sec") or 3600))
    except ValueError:
        return _err("拉取间隔必须是数字（秒）")

    pkg.name = name
    old_url = pkg.source_url
    pkg.source_url = (form.get("source_url") or "").strip()
    if pkg.source_url and pkg.source_url != old_url:
        pkg.origin = "url"  # 改了来源 URL 则回到 URL 拉取模式（粘贴内容保存时仍会覆盖为 paste）
    pkg.poll_interval_sec = poll_interval
    auth_header = (form.get("auth_header") or "").strip()
    if auth_header:
        pkg.auth_header = encrypt(auth_header)
    db.add(pkg)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return _err(f"已存在同名 proto 包: {name}")

    content = (form.get("content") or "").strip()
    if content:
        pkg.origin = "paste"  # 粘贴了新内容则来源方式变为手动粘贴
        await run_in_threadpool(proto_center.update_content, db, pkg, content)
        if pkg.status == "error":
            return goto(request, f"/protos/{pkg.id}",
                        f"基本信息已保存，但 proto 解析失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg.id}", f"proto 包 {name} 已更新")


@router.post("/protos/{pkg_id}/poll")
def proto_poll(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """立即从 source_url 拉取最新 proto。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    if not pkg.source_url:
        return goto(request, f"/protos/{pkg_id}", "该 proto 包未配置来源 URL", ok=False)
    proto_center.poll_package(db, pkg)
    if pkg.status == "error":
        return goto(request, f"/protos/{pkg_id}", f"拉取失败: {pkg.error}", ok=False)
    return goto(request, f"/protos/{pkg_id}", f"拉取完成，状态: {pkg.status}")


@router.delete("/protos/{pkg_id}")
def proto_delete(request: Request, pkg_id: int, db: Session = Depends(get_db)):
    """删除 proto 包；有作业引用时拒绝并列出引用作业名。"""
    pkg = db.get(ProtoPackage, pkg_id)
    if not pkg:
        return goto(request, "/protos", "proto 包不存在", ok=False)
    refs = db.query(Job).filter(Job.proto_package_id == pkg_id).all()
    if refs:
        names = "、".join(j.name for j in refs)
        return goto(request, "/protos",
                    f"proto 包 {pkg.name} 被 {len(refs)} 个作业引用（{names}），无法删除", ok=False)
    db.delete(pkg)
    db.commit()
    return goto(request, "/protos", f"proto 包 {pkg.name} 已删除")

