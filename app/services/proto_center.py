"""proto 包中心：拉取/解析 .proto，产出字段树、diff 与 SeaTunnel schema fields。"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from datetime import datetime

import httpx
from sqlalchemy.orm import Session

from ..core.crypto import decrypt
from ..models import ProtoPackage

# protoc 字段类型编号 -> proto 类型名（与 google.protobuf.FieldDescriptorProto.Type 对应）
_TYPE_NAMES = {
    1: "double", 2: "float", 3: "int64", 4: "uint64", 5: "int32", 6: "fixed64",
    7: "fixed32", 8: "bool", 9: "string", 10: "group", 11: "message", 12: "bytes",
    13: "uint32", 14: "enum", 15: "sfixed32", 16: "sfixed64", 17: "sint32", 18: "sint64",
}
_LABEL_NAMES = {1: "optional", 2: "required", 3: "repeated"}

# proto 标量类型 -> SeaTunnel 类型
# enum -> string：SeaTunnel ProtobufToRowConverter 对 INT 原样返回，而 protobuf-java
# 对 enum 字段返回 EnumValueDescriptor 对象（非 Integer），声明 int 会在下游类型错误；
# STRING 分支 toString() 恰好输出枚举名
_ST_SCALAR = {
    "double": "double", "float": "float",
    "int32": "int", "sint32": "int", "uint32": "int", "fixed32": "int", "sfixed32": "int",
    "int64": "bigint", "sint64": "bigint", "uint64": "bigint", "fixed64": "bigint", "sfixed64": "bigint",
    "bool": "boolean", "string": "string", "bytes": "bytes", "enum": "string",
}
_MAX_DEPTH = 10  # 自引用 message 的递归保护


def _compile_fds(content: str) -> "object":
    """用 grpc_tools.protoc 把 proto 文本编译成 FileDescriptorSet。"""
    from grpc_tools import protoc
    import grpc_tools
    from google.protobuf import descriptor_pb2

    include = os.path.join(grpc_tools.__path__[0], "_proto")
    with tempfile.TemporaryDirectory() as td:
        proto_path = os.path.join(td, "pkg.proto")
        out_path = os.path.join(td, "out.fds")
        with open(proto_path, "w", encoding="utf-8") as f:
            f.write(content)

        def _run(text: str) -> int:
            with open(proto_path, "w", encoding="utf-8") as f:
                f.write(text)
            return protoc.main([
                "protoc", f"-I{td}", f"-I{include}",
                f"--descriptor_set_out={out_path}", "--include_imports", proto_path,
            ])

        code = _run(content)
        if code != 0 and re.search(r'^\s*import\s', content, re.M):
            # 引用外部业务 proto 时本地没有依赖文件，去掉 import 行重试（类型会降级为未知）
            stripped = re.sub(r'^\s*import\s[^;]*;\s*$', '', content, flags=re.M)
            code = _run(stripped)
        if code != 0:
            raise ValueError("protoc 编译失败：请检查 proto 内容语法")
        fds = descriptor_pb2.FileDescriptorSet()
        with open(out_path, "rb") as f:
            fds.ParseFromString(f.read())
        return fds


def _scalar_or_named(field) -> str:
    """map key/value 的类型名（标量或 message/enum）。"""
    return _TYPE_NAMES.get(field.type, "string")


def _field_tree(msg, registry: dict, ancestors: frozenset) -> list[dict]:
    """把 DescriptorProto 转成嵌套字段树。"""
    fields = []
    for f in msg.field:
        entry = {
            "name": f.name,
            "proto_type": _TYPE_NAMES.get(f.type, "string"),
            "label": _LABEL_NAMES.get(f.label, "optional"),
            "type_name": "",
            "fields": [],
        }
        tname = f.type_name.lstrip(".") if f.type_name else ""
        if f.type in (10, 11):  # group / message
            target = registry.get(tname)
            if target is not None and target.options.map_entry:
                kf, vf = target.field[0], target.field[1]
                entry["proto_type"] = "map"
                entry["key_type"] = _scalar_or_named(kf)
                entry["value_type"] = _scalar_or_named(vf)
                if vf.type in (10, 11):
                    vtarget = registry.get(vf.type_name.lstrip("."))
                    if vtarget is not None and tname not in ancestors and len(ancestors) < _MAX_DEPTH:
                        entry["fields"] = _field_tree(vtarget, registry, ancestors | {vf.type_name.lstrip(".")})
            else:
                entry["proto_type"] = "message"
                entry["type_name"] = tname
                if target is not None and tname not in ancestors and len(ancestors) < _MAX_DEPTH:
                    entry["fields"] = _field_tree(target, registry, ancestors | {tname})
        elif f.type == 14:  # enum
            entry["proto_type"] = "enum"
            entry["type_name"] = tname
        fields.append(entry)
    return fields


def parse_proto(content: str) -> dict:
    """解析 .proto 文本，返回 {"messages": {名: field_tree}, "top_level": [顶层 message 名]}。

    messages 以简单名为主键（嵌套 message 也收录），重名时先到先得。
    """
    fds = _compile_fds(content)
    registry: dict = {}

    def _register(msg, prefix: str) -> None:
        full = f"{prefix}.{msg.name}" if prefix else msg.name
        registry[full] = msg
        for nested in msg.nested_type:
            _register(nested, full)

    for fd in fds.file:
        for msg in fd.message_type:
            _register(msg, fd.package)

    messages: dict = {}
    top_level: list[str] = []
    for fd in fds.file:
        for msg in fd.message_type:
            full = f"{fd.package}.{msg.name}" if fd.package else msg.name
            if msg.name in messages:
                continue
            messages[msg.name] = _field_tree(msg, registry, frozenset({full}))
            top_level.append(msg.name)
    return {"messages": messages, "top_level": top_level}


def diff_parsed(old: dict | None, new: dict) -> dict:
    """按 message 粒度比较两次解析产物（字段树整体不同即记 type_changed）。"""
    om = (old or {}).get("messages", {})
    nm = (new or {}).get("messages", {})
    changed: dict = {}
    for m in set(om) & set(nm):
        of = {f["name"]: f for f in om[m]}
        nf = {f["name"]: f for f in nm[m]}
        added = sorted(set(nf) - set(of))
        removed = sorted(set(of) - set(nf))
        type_changed = sorted(n for n in set(of) & set(nf) if of[n] != nf[n])
        if added or removed or type_changed:
            changed[m] = {"added_fields": added, "removed_fields": removed, "type_changed": type_changed}
    return {
        "added_messages": sorted(set(nm) - set(om)),
        "removed_messages": sorted(set(om) - set(nm)),
        "changed": changed,
    }


def _apply_content(pkg: ProtoPackage, content: str, version: str, changed_status: str) -> None:
    """共同的"新内容入库"逻辑：无变化置 current，有变化做 prev 轮换 + parse + diff。"""
    pkg.current_version = version
    pkg.last_polled_at = datetime.now()
    if pkg.content == content:
        pkg.status = "current"
        pkg.error = None
        return
    parsed = parse_proto(content)  # 解析失败抛异常，由调用方落 error
    old_parsed = pkg.parsed
    pkg.prev_content = pkg.content
    pkg.prev_parsed_json = pkg.parsed_json
    pkg.content = content
    pkg.parsed_json = pkg._dumps(parsed)
    pkg.diff_json = pkg._dumps(diff_parsed(old_parsed, parsed))
    pkg.status = changed_status
    pkg.error = None


def poll_package(db: Session, pkg: ProtoPackage) -> ProtoPackage:
    """从 source_url 拉取最新 proto，有变化时重新解析并记录 diff（status=updated）。"""
    try:
        headers = {}
        if pkg.auth_header:
            headers["Authorization"] = decrypt(pkg.auth_header)
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get(pkg.source_url, headers=headers)
            resp.raise_for_status()
        content = resp.text
        version = resp.headers.get("etag") or hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
        _apply_content(pkg, content, version, changed_status="updated")
    except Exception as e:  # noqa: BLE001 - 轮询失败只落库不抛出
        pkg.status = "error"
        pkg.error = str(e)[:2000]
        pkg.last_polled_at = datetime.now()
    db.add(pkg)
    db.commit()
    return pkg


def update_content(db: Session, pkg: ProtoPackage, content: str) -> ProtoPackage:
    """手动粘贴/上传 proto 内容，走与轮询相同的 parse+diff 逻辑（status=current）。"""
    try:
        version = hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]
        _apply_content(pkg, content, version, changed_status="current")
    except Exception as e:  # noqa: BLE001
        pkg.status = "error"
        pkg.error = str(e)[:2000]
        pkg.last_polled_at = datetime.now()
    db.add(pkg)
    db.commit()
    return pkg


def _st_type(field: dict) -> str:
    """字段树节点 -> SeaTunnel 类型字符串。"""
    pt = field["proto_type"]
    if pt == "message":
        inner = ",".join(f"{sf['name']}:{_st_type(sf)}" for sf in field.get("fields", []))
        t = "{" + inner + "}"
    elif pt == "map":
        key_t = _ST_SCALAR.get(field.get("key_type", "string"), "string")
        vt = field.get("value_type", "string")
        if vt in ("message", "group"):
            inner = ",".join(f"{sf['name']}:{_st_type(sf)}" for sf in field.get("fields", []))
            val_t = "{" + inner + "}"
        else:
            val_t = _ST_SCALAR.get(vt, "string")
        t = f"map<{key_t},{val_t}>"
    else:
        t = _ST_SCALAR.get(pt, "string")
    if field.get("label") == "repeated" and pt != "map":
        if pt == "bytes":
            t = "string"  # array<bytes> SeaTunnel 不支持，降级为 string
        else:
            t = f"array<{t}>"
    return t


def _message_tree(pkg: ProtoPackage, message_name: str) -> list[dict]:
    """取指定 message 的字段树（支持全限定名回退），不存在抛 KeyError。"""
    messages = (pkg.parsed or {}).get("messages", {})
    tree = messages.get(message_name)
    if tree is None:
        for key, val in messages.items():
            if key.split(".")[-1] == message_name:
                tree = val
                break
    if tree is None:
        raise KeyError(f"proto 包 {pkg.name} 中不存在 message: {message_name}")
    return tree


def schema_fields_for(pkg: ProtoPackage, message_name: str) -> list[dict]:
    """把指定 message 的字段树转成 SeaTunnel schema fields: [{"name", "st_type"}]。"""
    return [{"name": f["name"], "st_type": _st_type(f)} for f in _message_tree(pkg, message_name)]


def _flatten_into(out: list[dict], field: dict, path: str, prefix: str,
                  root: str, root_type: str) -> None:
    """递归展开嵌套 message：标量叶子记 src_path；repeated/map 后代不再下钻，整列保留。"""
    for child in field.get("fields", []):
        cpath = f"{path}.{child['name']}"
        cname = f"{prefix}_{child['name']}"
        if child.get("proto_type") == "message" and child.get("label") != "repeated":
            _flatten_into(out, child, cpath, cname, root, root_type)
        else:
            out.append({
                "name": cname, "st_type": _st_type(child), "src_path": cpath,
                "src_root": root, "src_root_type": root_type,
            })


def flattened_schema_fields(pkg: ProtoPackage, message_name: str, flatten: set[str]) -> list[dict]:
    """schema_fields_for 的拍平版：flatten 集合中的嵌套 message 字段（非 repeated）递归展开。

    展开项带 src_path（点号访问路径，供 SQL transform）与 src_root/src_root_type
    （顶层父字段名/类型，供 source schema 重建）；不在 flatten 中的字段保持原样。
    """
    out: list[dict] = []
    for f in _message_tree(pkg, message_name):
        if f["name"] in flatten and f.get("proto_type") == "message" \
                and f.get("label") != "repeated":
            _flatten_into(out, f, f["name"], f["name"], f["name"], _st_type(f))
        else:
            out.append({"name": f["name"], "st_type": _st_type(f)})
    return out


# ---------------------------------------------------------------- proto 裁剪（conf 只嵌入选中 message 及其依赖闭包）

_TOP_BLOCK_RE = re.compile(r'^\s*(message|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{')


def _brace_delta(line: str) -> int:
    """剥掉行内字符串字面量与 // 注释后的花括号净值（避免 "}" 之类的字面量干扰块边界）。"""
    out: list[str] = []
    i = 0
    in_str: str | None = None
    while i < len(line):
        ch = line[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == in_str:
                in_str = None
        else:
            if ch in ('"', "'"):
                in_str = ch
            elif ch == "/" and i + 1 < len(line) and line[i + 1] == "/":
                break
            else:
                out.append(ch)
        i += 1
    s = "".join(out)
    return s.count("{") - s.count("}")


def split_proto_blocks(content: str) -> dict:
    """把 .proto 文本拆成 {"header": 头部, "blocks": [{"kind","name","text"}]}。

    header = 所有不在顶层 message/enum 块内的行（syntax/package/import/option 等公共属性，
    全量保留——裁剪失败也不破坏可用性）；块 = 顶层 message/enum 完整原文（嵌套类型随父块自带）。
    块前的连续 // 注释（及空行）归属于其后的块——被剔除 message 的注释不会混进 header。
    """
    blocks: list[dict] = []
    header_lines: list[str] = []
    pending: list[str] = []  # 紧邻块之前的注释/空行缓冲
    cur: dict | None = None
    for line in (content or "").splitlines():
        if cur is None:
            m = _TOP_BLOCK_RE.match(line)
            if m:
                cur = {"kind": m.group(1), "name": m.group(2),
                       "lines": pending + [line],
                       "depth": _brace_delta(line)}
                pending = []
                if cur["depth"] <= 0:  # 单行 message X {}
                    blocks.append({"kind": cur["kind"], "name": cur["name"],
                                   "text": "\n".join(cur["lines"])})
                    cur = None
                continue
            stripped = line.strip()
            if stripped.startswith("//") or not stripped:
                pending.append(line)
            else:
                header_lines.extend(pending)
                pending = []
                header_lines.append(line)
        else:
            cur["lines"].append(line)
            cur["depth"] += _brace_delta(line)
            if cur["depth"] <= 0:
                blocks.append({"kind": cur["kind"], "name": cur["name"],
                               "text": "\n".join(cur["lines"])})
                cur = None
    if cur is not None:  # 括号未闭合（proto 本身有问题）：退回 header，不裁剪
        header_lines.extend(cur["lines"])
        blocks = []
    header_lines.extend(pending)
    return {"header": "\n".join(header_lines).strip("\n"), "blocks": blocks}


def subset_proto(content: str, message_name: str) -> str:
    """裁剪 proto：头部 + 选中 message 及其依赖闭包（文本级 BFS）。

    依赖识别：块文本中出现其他顶层块的简单名（限定名 `pkg.A.B` 的段匹配也算）。
    任何异常（块解析失败/找不到目标块）直接返回原文——可用性优先于精简。
    """
    parts = split_proto_blocks(content)
    blocks = parts["blocks"]
    by_name = {b["name"]: b for b in blocks}
    if not blocks or message_name not in by_name:
        return content
    keep: set[str] = set()
    queue = [message_name]
    while queue:
        name = queue.pop()
        if name in keep or name not in by_name:
            continue
        keep.add(name)
        text = by_name[name]["text"]
        for other in by_name:
            if other not in keep and re.search(rf"\b{re.escape(other)}\b", text):
                queue.append(other)
    body = "\n\n".join(b["text"] for b in blocks if b["name"] in keep)
    return parts["header"] + "\n\n" + body + "\n"
