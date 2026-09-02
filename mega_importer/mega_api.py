"""
MEGA HTTP API Client for public links (Files & Folders).
Direct communication with https://g.api.mega.co.nz/cs without external daemons.
"""
from __future__ import annotations

import json
import random
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple

import requests

from .helpers import add_log, sanitize_filename
from .mega_crypto import (
    a32_to_base64,
    base64_to_a32,
    base64_url_decode,
    decrypt_attr,
    decrypt_key,
)

API_URL = "https://g.api.mega.co.nz/cs"
_SEQ_LOCK = threading.Lock()
_SEQUENCE_NUMBER = random.randint(1000, 0xFFFFFF)

_H = r"[A-Za-z0-9_-]+"
_K = r"[A-Za-z0-9_,-]+"

_FOLDER_FILE_RE = re.compile(rf"/folder/({_H})#({_K})/file/({_H})")
_FOLDER_RE = re.compile(rf"/folder/({_H})#({_K})")
_LEGACY_FOLDER_RE = re.compile(rf"#F!({_H})!({_K})(?:!({_H}))?")
_FILE_RE = re.compile(rf"/file/({_H})#({_K})")
_LEGACY_FILE_RE = re.compile(rf"#!({_H})!({_K})")


def _next_seq() -> int:
    global _SEQUENCE_NUMBER
    with _SEQ_LOCK:
        seq = _SEQUENCE_NUMBER
        _SEQUENCE_NUMBER += 1
    return seq


@dataclass
class ResolvedFile:
    file_handle: str
    file_name: str
    file_size: int
    key_a32: Tuple[int, ...]
    key_b64: str
    cdn_url: str
    folder_id: Optional[str] = None
    rel_path: Optional[str] = None


@dataclass
class ResolvedFolderItem:
    node_handle: str
    file_name: str
    file_size: int
    key_a32: Tuple[int, ...]
    key_b64: str
    rel_path: str  # e.g. "subfolder/image.png" or "image.png"


@dataclass
class ResolvedFolder:
    folder_id: str
    folder_name: str
    items: List[ResolvedFolderItem]
    total_bytes: int


def parse_mega_url(url: str) -> dict:
    """
    Parse a MEGA URL and return link type and metadata.
    Returns:
      {
        "type": "file" | "folder",
        "handle": str,
        "key": str,
        "folder_id": str | None,
        "node_id": str | None
      }
    """
    url = url.strip()

    # 1. Folder with single file selected: /folder/<id>#<key>/file/<node>
    m = _FOLDER_FILE_RE.search(url)
    if m:
        return {
            "type": "folder_item",
            "folder_id": m.group(1),
            "key": m.group(2),
            "node_id": m.group(3),
        }

    # 2. Entire Folder: /folder/<id>#<key>
    m = _FOLDER_RE.search(url)
    if m:
        return {
            "type": "folder",
            "folder_id": m.group(1),
            "key": m.group(2),
            "node_id": None,
        }

    # 3. Legacy Folder: #F!<id>!<key> or #F!<id>!<key>!<node>
    m = _LEGACY_FOLDER_RE.search(url)
    if m:
        if m.group(3):
            return {
                "type": "folder_item",
                "folder_id": m.group(1),
                "key": m.group(2),
                "node_id": m.group(3),
            }
        return {
            "type": "folder",
            "folder_id": m.group(1),
            "key": m.group(2),
            "node_id": None,
        }

    # 4. Modern File: /file/<handle>#<key>
    m = _FILE_RE.search(url)
    if m:
        return {
            "type": "file",
            "handle": m.group(1),
            "key": m.group(2),
            "folder_id": None,
            "node_id": None,
        }

    # 5. Legacy File: #!<handle>!<key>
    m = _LEGACY_FILE_RE.search(url)
    if m:
        return {
            "type": "file",
            "handle": m.group(1),
            "key": m.group(2),
            "folder_id": None,
            "node_id": None,
        }

    raise ValueError(f"Неизвестный формат ссылки MEGA: {url}")


class MegaApiClient:
    """Public MEGA API client."""

    def __init__(self, proxies: Optional[dict] = None, timeout: float = 30.0):
        self.session = requests.Session()
        self.timeout = timeout
        if proxies:
            self.session.proxies.update(proxies)

    def _api_request(self, payload: dict, extra_params: Optional[dict] = None) -> dict | list:
        seq = _next_seq()
        params = {"id": seq}
        if extra_params:
            params.update(extra_params)

        for attempt in range(3):
            try:
                resp = self.session.post(
                    API_URL,
                    params=params,
                    json=[payload],
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    res = data[0]
                    if isinstance(res, int) and res < 0:
                        if res == -9:
                            raise RuntimeError(f"MEGA API: Объект не найден или удален (код {res})")
                        if res == -16:
                            raise RuntimeError(f"MEGA API: Ресурс заблокирован или недоступен (код {res})")
                        if res == -3:
                            time.sleep(1 + attempt)
                            continue
                        raise RuntimeError(f"MEGA API ошибка (код {res})")
                    return res
                return data
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(1 + attempt)
        raise RuntimeError("MEGA API: Превышено число попыток запроса")

    def resolve_file(
        self,
        handle: str,
        key_b64: str,
        folder_id: Optional[str] = None,
    ) -> ResolvedFile:
        """Resolve a single MEGA file to get its CDN download URL and metadata."""
        key_a32 = base64_to_a32(key_b64)

        if folder_id:
            # File inside a shared folder
            payload = {"a": "g", "g": 1, "ssl": 2, "n": handle}
            extra = {"n": folder_id}
        else:
            # Standalone public file
            payload = {"a": "g", "g": 1, "ssl": 2, "p": handle}
            extra = None

        res = self._api_request(payload, extra_params=extra)
        if not isinstance(res, dict) or "g" not in res:
            raise RuntimeError(f"MEGA API: Не удалось получить CDN URL для файла {handle}: {res}")

        cdn_url = res["g"]
        file_size = int(res.get("s", 0))
        attr_blob = res.get("at")
        file_name = f"mega_file_{handle}"

        if attr_blob:
            attr_data = base64_url_decode(attr_blob)
            attrs = decrypt_attr(attr_data, key_a32)
            if attrs and "n" in attrs:
                file_name = sanitize_filename(attrs["n"])

        return ResolvedFile(
            file_handle=handle,
            file_name=file_name,
            file_size=file_size,
            key_a32=key_a32,
            key_b64=key_b64,
            cdn_url=cdn_url,
            folder_id=folder_id,
        )

    def resolve_folder(self, folder_id: str, key_b64: str) -> ResolvedFolder:
        """
        List and resolve a full MEGA folder into a list of downloadable items with relative paths.
        """
        master_key = base64_to_a32(key_b64)
        payload = {"a": "f", "c": 1, "r": 1}
        extra = {"n": folder_id}

        res = self._api_request(payload, extra_params=extra)
        if not isinstance(res, dict) or "f" not in res:
            raise RuntimeError(f"MEGA API: Ошибка получения дерева папки {folder_id}: {res}")

        nodes = res["f"]
        node_map = {}
        items: List[ResolvedFolderItem] = []
        folder_name = f"folder_{folder_id}"

        # 1. First pass: decrypt keys and attributes for all nodes
        for node in nodes:
            h = node.get("h")
            p = node.get("p")
            t = node.get("t", 0)  # 0=file, 1=dir, 2=root
            k_field = node.get("k", "")
            a_field = node.get("a", "")
            s = int(node.get("s", 0))

            if not h:
                continue

            node_key = None
            name = None

            if a_field:
                a_bytes = base64_url_decode(a_field)

                # 1. Проверяем все блоки ключей в поле 'k' (формат owner:blob/owner:blob)
                if k_field:
                    for part in k_field.split("/"):
                        _, sep, blob = part.partition(":")
                        candidate_blob = blob if (sep and blob) else part
                        try:
                            cand_k = decrypt_key(base64_to_a32(candidate_blob), master_key)
                            attrs = decrypt_attr(a_bytes, cand_k)
                            if attrs and "n" in attrs:
                                node_key = cand_k
                                name = sanitize_filename(attrs["n"])
                                break
                        except Exception:
                            continue

                # 2. Если имя не расшифровано по k_field, пробуем master_key напрямую (для корневого узла)
                if not name:
                    try:
                        attrs = decrypt_attr(a_bytes, master_key)
                        if attrs and "n" in attrs:
                            node_key = master_key
                            name = sanitize_filename(attrs["n"])
                    except Exception:
                        pass

            if not name:
                name = f"node_{h}"

            node_map[h] = {
                "handle": h,
                "parent": p,
                "type": t,
                "name": name,
                "size": s,
                "key": node_key or master_key,
            }

        # 2. Determine root folder node of the share
        all_handles = set(node_map.keys())
        root_handle = None
        for h, n in node_map.items():
            if n["type"] == 2:
                root_handle = h
                break
        if not root_handle:
            # Find directory node whose parent is outside the share list
            orphan_dirs = [h for h, n in node_map.items() if n["type"] == 1 and n["parent"] not in all_handles]
            if orphan_dirs:
                root_handle = orphan_dirs[0]

        if root_handle and root_handle in node_map:
            folder_name = node_map[root_handle]["name"]
        else:
            folder_name = f"folder_{folder_id}"

        # 3. Build relative paths stopping at root_handle
        def get_rel_path(handle: str) -> str:
            parts = []
            curr = handle
            while curr and curr in node_map:
                if curr == root_handle:
                    break
                n = node_map[curr]
                if n["type"] == 2:
                    break
                parts.append(n["name"])
                curr = n["parent"]
            parts.reverse()
            return "/".join(parts) if parts else node_map[handle]["name"]

        total_bytes = 0
        for h, n in node_map.items():
            if n["type"] == 0:  # file
                rel_p = get_rel_path(h)
                k_a32 = n["key"]
                if not k_a32:
                    continue
                k_b64 = a32_to_base64(k_a32)
                items.append(
                    ResolvedFolderItem(
                        node_handle=h,
                        file_name=n["name"],
                        file_size=n["size"],
                        key_a32=k_a32,
                        key_b64=k_b64,
                        rel_path=rel_p,
                    )
                )
                total_bytes += n["size"]

        return ResolvedFolder(
            folder_id=folder_id,
            folder_name=folder_name,
            items=items,
            total_bytes=total_bytes,
        )

    def inspect_folder_tree(self, folder_id: str, key_b64: str) -> dict:
        """
        Build a full hierarchical tree of a MEGA folder for UI exploration and item selection.
        """
        resolved = self.resolve_folder(folder_id, key_b64)

        def _dict_to_list(d: dict) -> list:
            nodes = []
            for k, v in d.items():
                if v["type"] == "folder":
                    nodes.append({
                        "name": v["name"],
                        "type": "folder",
                        "path": v["path"],
                        "size": v["size"],
                        "children": _dict_to_list(v["children"]),
                    })
                else:
                    nodes.append({
                        "name": v["name"],
                        "type": "file",
                        "path": v["path"],
                        "size": v["size"],
                    })
            nodes.sort(key=lambda x: (0 if x["type"] == "folder" else 1, x["name"].lower()))
            return nodes

        tree_dict: dict = {}
        for it in resolved.items:
            parts = it.rel_path.split("/")
            curr = tree_dict
            for i, p in enumerate(parts[:-1]):
                if p not in curr:
                    curr[p] = {
                        "type": "folder",
                        "name": p,
                        "children": {},
                        "size": 0,
                        "path": "/".join(parts[: i + 1]),
                    }
                curr[p]["size"] += it.file_size
                curr = curr[p]["children"]
            fname = parts[-1]
            curr[fname] = {
                "type": "file",
                "name": fname,
                "size": it.file_size,
                "path": it.rel_path,
            }

        # Calculate adaptive segments (~2.5-3.5 GB batches)
        from .worker import build_adaptive_batches
        batches = build_adaptive_batches(resolved.items)
        segments = []
        for idx, (b_name, b_items) in enumerate(batches.items(), 1):
            segments.append({
                "index": idx,
                "name": b_name,
                "count": len(b_items),
                "size": sum(it.file_size for it in b_items),
                "sample_files": [it.file_name for it in b_items[:4]],
            })

        return {
            "folder_name": resolved.folder_name,
            "total_bytes": resolved.total_bytes,
            "total_files": len(resolved.items),
            "tree": _dict_to_list(tree_dict),
            "segments": segments,
        }

