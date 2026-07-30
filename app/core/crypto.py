"""敏感字段对称加密（Fernet）。密钥存 data/secret.key，自动生成。"""
from __future__ import annotations

import re

from cryptography.fernet import Fernet

from .config import SECRET_KEY_PATH


def _load_key() -> bytes:
    if SECRET_KEY_PATH.exists():
        return SECRET_KEY_PATH.read_bytes().strip()
    key = Fernet.generate_key()
    SECRET_KEY_PATH.write_bytes(key)
    return key


_fernet = Fernet(_load_key())


def encrypt(plain: str) -> str:
    if not plain:
        return ""
    return _fernet.encrypt(plain.encode("utf-8")).decode("ascii")


def decrypt(token: str) -> str:
    if not token:
        return ""
    return _fernet.decrypt(token.encode("ascii")).decode("utf-8")


def decrypt_safe(token: str) -> str:
    """解密；非密文（历史明文数据）按原文返回。"""
    if not token:
        return ""
    try:
        return decrypt(token)
    except Exception:  # noqa: BLE001
        return token


def mask(plain: str) -> str:
    """UI 展示用掩码。"""
    if not plain:
        return ""
    if len(plain) <= 4:
        return "****"
    return plain[:2] + "****" + plain[-2:]


_SECRET_KEY = re.compile(r"password|auth", re.IGNORECASE)


def decrypt_conn(conn: dict) -> dict:
    """connection dict 中敏感字段（key 含 password/auth）解密后返回新 dict；非密文按原文。

    数据源/环境连接信息统一走这里解密，各服务层不要再自己实现。
    """
    out = {}
    for k, v in conn.items():
        if isinstance(v, str) and v and _SECRET_KEY.search(k):
            try:
                v = decrypt(v)
            except Exception:  # noqa: BLE001 - 历史明文按原文使用
                pass
        out[k] = v
    return out


def mask_conn(conn: dict) -> dict:
    """connection dict 展示版：密码字段解密后掩码（绝不回传明文/密文）。"""
    out = {}
    for k, v in conn.items():
        if isinstance(v, str) and v and "password" in k.lower():
            try:
                out[k] = mask(decrypt(v)) or "(空)"
            except Exception:  # noqa: BLE001
                out[k] = "****"
        else:
            out[k] = v
    return out


_URI_PWD = re.compile(r"(://[^:/\s]+):[^@\s]+@")


def sanitize_error(msg: str) -> str:
    """错误消息脱敏：URI 内嵌密码（如 mongodb://user:pwd@host）的密码段打码。

    pymongo 等驱动的异常消息会带完整连接串，落库/展示前统一过一道。
    """
    if not msg:
        return msg
    return _URI_PWD.sub(r"\1:****@", msg)
