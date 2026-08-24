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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .helpers import add_log, format_bytes, update_state
from .mega_api import MegaApiClient, ResolvedFile, ResolvedFolderItem, parse_mega_url
from .mega_crypto import create_aes_ctr_cipher, derive_file_key
from .proxy import proxy_manager
from .state import stop_event, update_task

CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB chunks
MAX_CHUNK_RETRIES = 10
PARALLEL_FOLDER_WORKERS = 8  # Parallel file downloads for folders
PARALLEL_CHUNK_WORKERS = 4   # Parallel chunk downloads for large single files


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


def _create_session(proxies: Optional[dict] = None) -> requests.Session:
    """Create a persistent requests.Session with connection pooling."""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=Retry(total=2, backoff_factor=0.2))
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if proxies:
        session.proxies.update(proxies)
    return session


class ProgressTracker:
    """Global thread-safe progress and speed tracker across all files/chunks."""

    def __init__(self, total_bytes: int, task_id: Optional[str] = None):
        self.total_bytes = total_bytes
        self.task_id = task_id
        self.downloaded_bytes = 0
        self.completed_files = 0
        self.total_files = 1
        self.start_time = time.time()
        self.last_update_time = 0.0
        self._lock = threading.Lock()

    def add_bytes(self, num_bytes: int) -> None:
        with self._lock:
            self.downloaded_bytes += num_bytes
            now = time.time()
            if now - self.last_update_time < 0.6:
                return
            self.last_update_time = now

            elapsed = max(0.1, now - self.start_time)
            speed = self.downloaded_bytes / elapsed
            pct = (self.downloaded_bytes / self.total_bytes * 100) if self.total_bytes > 0 else 0
            pct = min(100.0, max(0.0, pct))
            speed_str = f"{format_bytes(int(speed))}/s"
            prog_str = f"{format_bytes(self.downloaded_bytes)} / {format_bytes(self.total_bytes)}"

            file_info = f"[{self.completed_files}/{self.total_files} файлов] " if self.total_files > 1 else ""
            msg = f"⏳ Скачивание: {file_info}{pct:.1f}% ({prog_str}) @ {speed_str}"

            update_state(progress=round(pct, 1), speed=speed_str, message=msg)
            if self.task_id:
                update_task(self.task_id, progress=round(pct, 1), speed=speed_str)

    def file_finished(self) -> None:
        with self._lock:
            self.completed_files += 1


class NativeFileDownloader:
    """Parallel chunk-based downloader with AES-CTR decryption and per-chunk proxy rotation."""

    def __init__(
        self,
        progress_tracker: ProgressTracker,
        concurrency: int = PARALLEL_CHUNK_WORKERS,
    ):
        self.progress = progress_tracker
        self.concurrency = max(1, concurrency)
        self._file_lock = threading.Lock()
        self._proxy_index = 0
        self._proxy_idx_lock = threading.Lock()
        self._direct_quota_hit = False

    def _pick_proxy(self) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Get (proxy_dict, requests_proxies).
        Uses direct connection first if available, otherwise cycles through available online proxies.
        """
        available = proxy_manager.get_available_proxies()
        
        # If user explicitly selected an active proxy in UI, use it
        if proxy_manager.active_proxy_id:
            active = next((p for p in available if p["id"] == proxy_manager.active_proxy_id), None)
            if active:
                return active, proxy_manager.build_requests_dict(active)

        # If direct connection hasn't hit quota yet, try direct first
        if not self._direct_quota_hit and not available:
            return None, None

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
        api: Optional[MegaApiClient] = None,
    ) -> Path:
        """Download and decrypt a resolved MEGA file."""
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        final_path = output_file_path
        part_path = final_path.with_suffix(final_path.suffix + ".part")

        file_size = resolved.file_size
        if file_size == 0:
            final_path.write_bytes(b"")
            self.progress.file_finished()
            return final_path

        key_bytes, iv_int = derive_file_key(resolved.key_a32)

        # Prepare .part file
        if not part_path.exists() or not _get_sidecar_path(part_path).exists():
            with open(part_path, "wb") as f:
                f.truncate(file_size)
            done_starts: Set[int] = set()
        else:
            done_starts = _load_progress(part_path, file_size)

        all_chunks = _split_into_chunks(file_size, CHUNK_SIZE)
        pending_chunks = [c for c in all_chunks if c[0] not in done_starts]

        # Account for already finished chunks
        initial_bytes = sum((end - start + 1) for start, end in all_chunks if start in done_starts)
        if initial_bytes > 0:
            self.progress.add_bytes(initial_bytes)

        # For small files (<= 1 chunk), download synchronously without creating thread pool
        if len(pending_chunks) <= 1:
            for start, end in pending_chunks:
                self._download_chunk(resolved, part_path, start, end, key_bytes, iv_int, api=api)
                done_starts.add(start)
                _save_progress(part_path, file_size, done_starts)
        else:
            # Download large file chunks in parallel
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                futures = {
                    executor.submit(
                        self._download_chunk,
                        resolved,
                        part_path,
                        start,
                        end,
                        key_bytes,
                        iv_int,
                        api=api,
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
                        done_starts.add(start)
                        _save_progress(part_path, file_size, done_starts)
                    except Exception as e:
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise RuntimeError(f"Ошибка загрузки блока {start}-{end}: {e}") from e

        # All chunks completed!
        _get_sidecar_path(part_path).unlink(missing_ok=True)
        if final_path.exists():
            final_path.unlink()
        os.replace(part_path, final_path)

        self.progress.file_finished()
        return final_path

    def _download_chunk(
        self,
        resolved: ResolvedFile,
        part_path: Path,
        start: int,
        end: int,
        key_bytes: bytes,
        iv_int: int,
        api: Optional[MegaApiClient] = None,
    ) -> None:
        """Download, decrypt, and write a single byte range."""
        chunk_len = end - start + 1
        headers = {"Range": f"bytes={start}-{end}"}
        current_cdn_url = resolved.cdn_url

        for attempt in range(MAX_CHUNK_RETRIES):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            # Re-resolve fresh CDN URL if retried multiple times
            if attempt >= 3 and api is not None:
                try:
                    fresh = api.resolve_file(resolved.file_handle, resolved.key_b64, folder_id=resolved.folder_id)
                    current_cdn_url = fresh.cdn_url
                except Exception:
                    pass

            proxy_info, req_proxies = self._pick_proxy()
            proxy_name = (
                f"{proxy_info['host']}:{proxy_info['port']}"
                if proxy_info
                else "Прямое соединение"
            )

            try:
                session = _create_session(req_proxies)
                with session.get(
                    current_cdn_url,
                    headers=headers,
                    stream=True,
                    timeout=(10, 40),
                ) as resp:
                    if resp.status_code in (429, 509):
                        # Quota exceeded on this proxy/IP
                        if proxy_info:
                            proxy_manager.mark_proxy_quota(proxy_info["id"], "Квота MEGA (HTTP 509)")
                            add_log(f"⚠️ Квота на прокси {proxy_name}. Ротация...", "WARNING")
                        else:
                            self._direct_quota_hit = True
                            add_log("⏳ Квота на прямом IP. Переключаюсь на прокси-пул...", "WARNING")
                        time.sleep(1)
                        continue

                    if resp.status_code == 403:
                        # Expired CDN token — refresh immediately
                        if api is not None:
                            try:
                                fresh = api.resolve_file(resolved.file_handle, resolved.key_b64, folder_id=resolved.folder_id)
                                current_cdn_url = fresh.cdn_url
                            except Exception:
                                pass
                        time.sleep(1)
                        continue

                    if resp.status_code not in (200, 206):
                        raise RuntimeError(f"HTTP {resp.status_code} от CDN")

                    # Decrypt cipher initialized at chunk start offset
                    cipher = create_aes_ctr_cipher(key_bytes, iv_int, byte_offset=start)
                    decrypted_buffer = bytearray()

                    for chunk_bytes in resp.iter_content(chunk_size=131072):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")
                        if not chunk_bytes:
                            continue

                        decrypted_piece = cipher.decrypt(chunk_bytes)
                        decrypted_buffer.extend(decrypted_piece)
                        self.progress.add_bytes(len(chunk_bytes))

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
    downloaded_files: List[Path] = []

    if link_type == "file":
        # 1. Single File
        resolved = api.resolve_file(parsed["handle"], parsed["key"])
        out_path = target_dir / resolved.file_name
        tracker = ProgressTracker(resolved.file_size, task_id=task_id)
        tracker.total_files = 1
        downloader = NativeFileDownloader(tracker, concurrency=PARALLEL_CHUNK_WORKERS)

        res_file = downloader.download_file(resolved, out_path, api=api)
        downloaded_files.append(res_file)
        add_log(f"✅ Файл сохранён: {resolved.file_name} ({format_bytes(resolved.file_size)})", "OK")

    elif link_type == "folder_item":
        # 2. Single item inside a shared folder
        resolved = api.resolve_file(
            parsed["node_id"],
            parsed["key"],
            folder_id=parsed["folder_id"],
        )
        out_path = target_dir / resolved.file_name
        tracker = ProgressTracker(resolved.file_size, task_id=task_id)
        tracker.total_files = 1
        downloader = NativeFileDownloader(tracker, concurrency=PARALLEL_CHUNK_WORKERS)

        res_file = downloader.download_file(resolved, out_path, api=api)
        downloaded_files.append(res_file)
        add_log(f"✅ Файл сохранён: {resolved.file_name} ({format_bytes(resolved.file_size)})", "OK")

    elif link_type == "folder":
        # 3. Full Folder with parallel multi-file downloading
        folder_id = parsed["folder_id"]
        key_b64 = parsed["key"]

        add_log(f"📂 Получение структуры папки MEGA {folder_id}...", "INFO")
        resolved_folder = api.resolve_folder(folder_id, key_b64)
        total_items = len(resolved_folder.items)
        add_log(
            f"📂 Папка: «{resolved_folder.folder_name}» ({total_items} файлов, "
            f"{format_bytes(resolved_folder.total_bytes)})",
            "OK",
        )

        folder_root = target_dir / resolved_folder.folder_name
        tracker = ProgressTracker(resolved_folder.total_bytes, task_id=task_id)
        tracker.total_files = total_items
        downloader = NativeFileDownloader(tracker, concurrency=2)

        def _download_single_item(item: ResolvedFolderItem) -> Path:
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            for file_attempt in range(3):
                try:
                    file_resolved = api.resolve_file(
                        item.node_handle,
                        item.key_b64,
                        folder_id=folder_id,
                    )
                    out_p = folder_root / item.rel_path
                    return downloader.download_file(file_resolved, out_p, api=api)
                except Exception as e:
                    if stop_event.is_set():
                        raise
                    if file_attempt == 2:
                        raise
                    time.sleep(1 + file_attempt)

        # Parallel file downloading pool
        workers_count = min(PARALLEL_FOLDER_WORKERS, max(2, total_items))
        add_log(f"⚡ Параллельное скачивание: {workers_count} одновременных потоков", "INFO")

        with ThreadPoolExecutor(max_workers=workers_count) as executor:
            futures = {
                executor.submit(_download_single_item, item): item
                for item in resolved_folder.items
            }

            for future in as_completed(futures):
                if stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise RuntimeError("Остановлено пользователем")

                item = futures[future]
                try:
                    res_p = future.result()
                    downloaded_files.append(res_p)
                except Exception as e:
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise RuntimeError(f"Ошибка скачивания файла {item.rel_path}: {e}") from e

        add_log(
            f"✅ Скачивание папки завершено: {len(downloaded_files)} файлов "
            f"({format_bytes(resolved_folder.total_bytes)})",
            "OK",
        )

    return downloaded_files

