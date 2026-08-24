"""
native_downloader.py — High-performance Native MEGA Downloader with Per-Chunk Proxy Rotation.
Directly interfaces with MEGA HTTP CDN via Range requests + on-the-fly AES-128-CTR decryption.
"""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, List, Optional, Set, Tuple

import requests

from .helpers import add_log, format_bytes, update_state
from .mega_api import MegaApiClient, ResolvedFile, parse_mega_url
from .mega_crypto import create_aes_ctr_cipher, derive_file_key
from .proxy import proxy_manager
from .state import stop_event, update_task

CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB chunks
MAX_CHUNK_RETRIES = 10
DEFAULT_CONCURRENCY = 4


def _align_down(n: int, block: int = 16) -> int:
    return (n // block) * block


def _split_into_chunks(file_size: int, chunk_size: int = CHUNK_SIZE) -> List[Tuple[int, int]]:
    """Split file_size into 16-byte aligned byte ranges [start, end]."""
    if file_size <= 0:
        return []
    chunks = []
    start = 0
    while start < file_size:
        end = min(start + chunk_size - 1, file_size - 1)
        chunks.append((start, end))
        start = end + 1
    return chunks


def _get_sidecar_path(part_path: Path) -> Path:
    return part_path.with_name(part_path.name + ".progress.json")


def _load_progress(part_path: Path, file_size: int) -> Set[int]:
    """Load completed chunk start offsets from sidecar."""
    sidecar = _get_sidecar_path(part_path)
    if not sidecar.exists():
        return set()
    try:
        data = json.loads(sidecar.read_text(encoding="utf-8"))
        if data.get("file_size") == file_size:
            return set(data.get("done_starts", []))
    except Exception:
        pass
    return set()


def _save_progress(part_path: Path, file_size: int, done_starts: Set[int]) -> None:
    """Save completed chunk start offsets to sidecar."""
    sidecar = _get_sidecar_path(part_path)
    tmp = sidecar.with_suffix(".tmp")
    try:
        payload = {"file_size": file_size, "done_starts": sorted(list(done_starts))}
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, sidecar)
    except Exception:
        pass


class NativeFileDownloader:
    """Parallel chunk-based downloader with AES-CTR decryption and per-chunk proxy rotation."""

    def __init__(
        self,
        concurrency: int = DEFAULT_CONCURRENCY,
        task_id: Optional[str] = None,
    ):
        self.concurrency = max(1, concurrency)
        self.task_id = task_id
        self._file_lock = threading.Lock()
        self._progress_lock = threading.Lock()
        self._bytes_downloaded = 0
        self._total_bytes = 0
        self._start_time = time.time()
        self._last_log_time = 0.0
        self._proxy_index = 0
        self._proxy_idx_lock = threading.Lock()

    def _pick_proxy(self) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Get (proxy_dict, requests_proxies).
        Cycles through available online proxies.
        """
        available = proxy_manager.get_available_proxies()
        if not available:
            return None, None

        with self._proxy_idx_lock:
            self._proxy_index = (self._proxy_index + 1) % len(available)
            p = available[self._proxy_index]
            return p, proxy_manager.build_requests_dict(p)

    def download_file(
        self,
        resolved: ResolvedFile,
        output_file_path: Path,
    ) -> Path:
        """Download and decrypt a resolved MEGA file."""
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        final_path = output_file_path
        part_path = final_path.with_suffix(final_path.suffix + ".part")

        file_size = resolved.file_size
        self._total_bytes = file_size
        self._start_time = time.time()

        if file_size == 0:
            final_path.write_bytes(b"")
            return final_path

        key_bytes, iv_int = derive_file_key(resolved.key_a32)

        # Pre-allocate / prepare .part file
        if not part_path.exists() or not _get_sidecar_path(part_path).exists():
            # Create or truncate
            with open(part_path, "wb") as f:
                f.truncate(file_size)
            done_starts: Set[int] = set()
        else:
            done_starts = _load_progress(part_path, file_size)

        all_chunks = _split_into_chunks(file_size, CHUNK_SIZE)
        pending_chunks = [c for c in all_chunks if c[0] not in done_starts]

        # Calculate initial bytes
        initial_bytes = sum((end - start + 1) for start, end in all_chunks if start in done_starts)
        self._bytes_downloaded = initial_bytes

        if pending_chunks:
            add_log(
                f"📥 Скачивание: {final_path.name} ({format_bytes(file_size)}) "
                f"[{len(pending_chunks)}/{len(all_chunks)} блоков]",
                "INFO",
            )

        # Download chunks in thread pool
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = {
                executor.submit(
                    self._download_chunk,
                    resolved.cdn_url,
                    part_path,
                    start,
                    end,
                    key_bytes,
                    iv_int,
                ): (start, end)
                for start, end in pending_chunks
            }

            for future in as_completed(futures):
                if stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise RuntimeError("Остановлено пользователем")

                start, end = futures[future]
                try:
                    future.result()
                    with self._progress_lock:
                        done_starts.add(start)
                        _save_progress(part_path, file_size, done_starts)
                except Exception as e:
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise RuntimeError(f"Ошибка загрузки блока {start}-{end}: {e}") from e

        # All chunks completed successfully!
        _get_sidecar_path(part_path).unlink(missing_ok=True)
        if final_path.exists():
            final_path.unlink()
        os.replace(part_path, final_path)

        add_log(f"✅ Завершено: {final_path.name} ({format_bytes(file_size)})", "OK")
        return final_path

    def _download_chunk(
        self,
        cdn_url: str,
        part_path: Path,
        start: int,
        end: int,
        key_bytes: bytes,
        iv_int: int,
    ) -> None:
        """Download, decrypt, and write a single byte range."""
        chunk_len = end - start + 1
        headers = {"Range": f"bytes={start}-{end}"}

        for attempt in range(MAX_CHUNK_RETRIES):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            proxy_info, req_proxies = self._pick_proxy()
            proxy_name = (
                f"{proxy_info['host']}:{proxy_info['port']}"
                if proxy_info
                else "Прямое соединение"
            )

            try:
                with requests.get(
                    cdn_url,
                    headers=headers,
                    proxies=req_proxies,
                    stream=True,
                    timeout=(12, 45),
                ) as resp:
                    if resp.status_code in (429, 509):
                        # Quota exceeded on this proxy/IP
                        if proxy_info:
                            proxy_manager.mark_proxy_quota(proxy_info["id"], "Квота MEGA (HTTP 509)")
                            add_log(f"⚠️ Квота на прокси {proxy_name}. Ротация...", "WARNING")
                        else:
                            add_log("⏳ Квота на прямом IP. Пробую прокси...", "WARNING")
                        time.sleep(1)
                        continue

                    if resp.status_code not in (200, 206):
                        raise RuntimeError(f"HTTP {resp.status_code} от CDN")

                    # Decrypt cipher initialized at chunk start offset
                    cipher = create_aes_ctr_cipher(key_bytes, iv_int, byte_offset=start)
                    decrypted_buffer = bytearray()

                    for chunk_bytes in resp.iter_content(chunk_size=65536):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")
                        if not chunk_bytes:
                            continue

                        decrypted_piece = cipher.decrypt(chunk_bytes)
                        decrypted_buffer.extend(decrypted_piece)

                        # Update progress stats
                        with self._progress_lock:
                            self._bytes_downloaded += len(chunk_bytes)
                            self._report_progress()

                    if len(decrypted_buffer) != chunk_len:
                        raise RuntimeError(
                            f"Неполный блок: получено {len(decrypted_buffer)} из {chunk_len} байт"
                        )

                    # Thread-safe write to .part file
                    with self._file_lock:
                        with open(part_path, "r+b") as f:
                            f.seek(start)
                            f.write(decrypted_buffer)
                    return

            except Exception as e:
                if stop_event.is_set():
                    raise RuntimeError("Остановлено пользователем")

                if proxy_info:
                    proxy_manager.mark_proxy_offline(proxy_info["id"], str(e)[:60])
                time.sleep(1)

        raise RuntimeError(f"Не удалось скачать блок {start}-{end} после {MAX_CHUNK_RETRIES} попыток")

    def _report_progress(self) -> None:
        """Report speed and percentage to UI and state."""
        now = time.time()
        if now - self._last_log_time < 0.8:
            return
        self._last_log_time = now

        elapsed = max(0.1, now - self._start_time)
        speed = self._bytes_downloaded / elapsed
        pct = (self._bytes_downloaded / self._total_bytes * 100) if self._total_bytes > 0 else 0
        speed_str = f"{format_bytes(int(speed))}/s"
        prog_str = f"{format_bytes(self._bytes_downloaded)} / {format_bytes(self._total_bytes)}"

        update_state(
            progress=round(pct, 1),
            speed=speed_str,
            message=f"⏳ Скачивание: {pct:.1f}% ({prog_str}) @ {speed_str}",
        )
        if self.task_id:
            update_task(self.task_id, progress=round(pct, 1), speed=speed_str)


def download_mega_url(
    url: str,
    target_dir: Path,
    task_id: Optional[str] = None,
) -> List[Path]:
    """
    High-level entry point to download any MEGA URL (File, Folder, Folder-Item).
    Returns list of downloaded file paths.
    """
    parsed = parse_mega_url(url)
    link_type = parsed["type"]
    api = MegaApiClient()
    downloader = NativeFileDownloader(task_id=task_id)

    downloaded_files: List[Path] = []

    if link_type == "file":
        # 1. Single File
        resolved = api.resolve_file(parsed["handle"], parsed["key"])
        out_path = target_dir / resolved.file_name
        res_file = downloader.download_file(resolved, out_path)
        downloaded_files.append(res_file)

    elif link_type == "folder_item":
        # 2. Single item inside a shared folder
        resolved = api.resolve_file(
            parsed["node_id"],
            parsed["key"],
            folder_id=parsed["folder_id"],
        )
        out_path = target_dir / resolved.file_name
        res_file = downloader.download_file(resolved, out_path)
        downloaded_files.append(res_file)

    elif link_type == "folder":
        # 3. Full Folder
        folder_id = parsed["folder_id"]
        key_b64 = parsed["key"]

        add_log(f"📂 Получение списка файлов папки MEGA {folder_id}...", "INFO")
        resolved_folder = api.resolve_folder(folder_id, key_b64)
        add_log(
            f"📂 Папка: «{resolved_folder.folder_name}» ({len(resolved_folder.items)} файлов, "
            f"{format_bytes(resolved_folder.total_bytes)})",
            "OK",
        )

        folder_root = target_dir / resolved_folder.folder_name
        total_items = len(resolved_folder.items)

        for i, item in enumerate(resolved_folder.items, 1):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            add_log(f"[{i}/{total_items}] Разрешение ссылки: {item.rel_path}", "INFO")
            file_resolved = api.resolve_file(
                item.node_handle,
                item.key_b64,
                folder_id=folder_id,
            )
            out_path = folder_root / item.rel_path
            res_file = downloader.download_file(file_resolved, out_path)
            downloaded_files.append(res_file)

    return downloaded_files
