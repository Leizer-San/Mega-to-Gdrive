"""
pixeldrain.py — Полная поддержка сервиса Pixeldrain.com.

Возможности:
  - Парсинг и валидация URL файлов (/u/{id}) и списков (/l/{id})
  - API-клиент: получение метаданных файлов и коллекций
  - Инспектор структуры списков (дерево файлов + адаптивные сегменты)
  - Многопоточный загрузчик с Range-запросами, sidecar-прогрессом и ротацией прокси
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .helpers import add_log, format_bytes, sanitize_filename, update_state
from .proxy import proxy_manager
from .state import stop_event, update_task

# ── URL паттерны ───────────────────────────────────────────────────────────────

_PD_FILE_RE = re.compile(
    r"^https?://(?:www\.)?pixeldrain\.com/(?:u|api/file)/([A-Za-z0-9_-]+)(?:/.*)?$", re.I
)
_PD_LIST_RE = re.compile(
    r"^https?://(?:www\.)?pixeldrain\.com/(?:l|api/list)/([A-Za-z0-9_-]+)(?:/.*)?$", re.I
)

PIXELDRAIN_API = "https://pixeldrain.com/api"
PIXELDRAIN_CDN = "https://pixeldrain.com/api/file/{file_id}"

# ── Параметры загрузки ─────────────────────────────────────────────────────────

PD_CHUNK_SIZE = 8 * 1024 * 1024        # 8 MB (меньше чанки = лучше параллелизм)
PD_MAX_CHUNK_RETRIES = 10
PD_PARALLEL_FOLDER_WORKERS = 8         # параллельных файлов из списка (по умолчанию)
PD_PARALLEL_CHUNK_WORKERS = 4          # параллельных чанков одного файла

# ── Коды ошибок Pixeldrain ───────────────────────────────────────────────────
# Ошибки уровня IP/сети (ротируем прокси):
_PIXELDRAIN_IP_LIMIT_VALUES = {
    "ip_download_limited_captcha_required",
    "transfer_limit_exceeded",
    "download_limit_exceeded",
    "server_overload_captcha_required",
    "max_concurrent_downloads",
}

# Ошибки уровня конкретного файла (смена прокси бессмысленна):
_PIXELDRAIN_FILE_LIMIT_VALUES = {
    "file_rate_limited_captcha_required",
    "file_rate_limited",
    "file_viewer_only",
}


class PixeldrainFileRateLimitedError(RuntimeError):
    """Файл временно заблокирован сервером Pixeldrain (file_rate_limited_captcha_required)."""
    pass


# ── Dataclasses ────────────────────────────────────────────────────────────────

@dataclass
class PixeldrainFile:
    """Метаданные одного файла на Pixeldrain."""
    file_id: str
    name: str
    size: int


@dataclass
class PixeldrainList:
    """Метаданные коллекции/альбома Pixeldrain."""
    list_id: str
    title: str
    files: List[PixeldrainFile]

    @property
    def total_bytes(self) -> int:
        return sum(f.size for f in self.files)


# ── Валидация и парсинг URL ────────────────────────────────────────────────────

def is_pixeldrain_file_url(url: str) -> bool:
    """True если это ссылка на один файл Pixeldrain (/u/{id})."""
    return bool(_PD_FILE_RE.match(url.strip()))


def is_pixeldrain_list_url(url: str) -> bool:
    """True если это ссылка на коллекцию/альбом Pixeldrain (/l/{id})."""
    return bool(_PD_LIST_RE.match(url.strip()))


def is_pixeldrain_url(url: str) -> bool:
    """True если это любая поддерживаемая ссылка Pixeldrain."""
    u = url.strip()
    return bool(_PD_FILE_RE.match(u) or _PD_LIST_RE.match(u))


def parse_pixeldrain_url(url: str) -> dict:
    """
    Распарсить URL Pixeldrain и вернуть словарь с типом и ID.
    Возвращает:
      {"type": "file", "id": "..."}
      {"type": "list", "id": "..."}
    Raises ValueError если URL не распознан.
    """
    url = url.strip()
    m = _PD_LIST_RE.match(url)
    if m:
        return {"type": "list", "id": m.group(1)}
    m = _PD_FILE_RE.match(url)
    if m:
        return {"type": "file", "id": m.group(1)}
    raise ValueError(f"Не удалось распознать Pixeldrain URL: {url}")


# ── API-клиент Pixeldrain ──────────────────────────────────────────────────────

def get_pixeldrain_api_key() -> str:
    """Получить API-ключ Pixeldrain из STATE, config или переменной окружения."""
    from .state import STATE
    from . import config
    key = STATE.get("pixeldrain_api_key") or getattr(config, "PIXELDRAIN_API_KEY", "") or os.environ.get("PIXELDRAIN_API_KEY", "")
    return str(key).strip()


def get_pixeldrain_concurrency() -> int:
    """Получить количество параллельных потоков скачивания для папок Pixeldrain."""
    from .state import STATE
    val = STATE.get("pixeldrain_concurrency")
    if val:
        try:
            return max(1, min(64, int(val)))
        except (ValueError, TypeError):
            pass
    # Если есть API-ключ (Premium) — по умолчанию 16 потоков (Турбо), иначе 8
    if get_pixeldrain_api_key():
        return 16
    return 8


def get_pixeldrain_chunk_workers() -> int:
    """Количество параллельных чанков для одного файла."""
    if get_pixeldrain_api_key():
        return 8
    return 4


# Thread-local хранилище сессий — каждый поток имеет свою независимую сессию
# (requests.Session НЕ является thread-safe при одновременном использовании из нескольких потоков)
_thread_local = threading.local()


def _get_thread_session(proxies: Optional[dict] = None) -> requests.Session:
    """Вернуть сессию, привязанную к текущему потоку (thread-safe через threading.local)."""
    key = f"session_{str(sorted(proxies.items())) if proxies else 'direct'}"
    session = getattr(_thread_local, key, None)
    if session is None:
        session = _pd_session(proxies)
        setattr(_thread_local, key, session)
    return session


class PixeldrainSessionPool:
    """Stub для совместимости — теперь использует thread-local сессии."""

    @classmethod
    def get_session(cls, req_proxies: Optional[dict] = None) -> requests.Session:
        return _get_thread_session(req_proxies)

    @classmethod
    def clear(cls) -> None:
        # Сброс thread-local делается автоматически при завершении потока.
        # Просто очищаем все сессии из текущего потока.
        for attr in list(vars(_thread_local)):
            if attr.startswith("session_"):
                try:
                    getattr(_thread_local, attr).close()
                except Exception:
                    pass
                try:
                    delattr(_thread_local, attr)
                except Exception:
                    pass


def _pd_session(proxies: Optional[dict] = None) -> requests.Session:
    """Создать requests.Session с connection pooling и авторизацией Pixeldrain (при наличии ключа)."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=4,
        pool_maxsize=8,
        max_retries=Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504]),
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "MegaGdriveImporter/2.0",
        "Accept-Encoding": "identity",
        "Connection": "keep-alive",
    })
    if proxies:
        session.proxies.update(proxies)

    api_key = get_pixeldrain_api_key()
    if api_key:
        session.auth = ("", api_key)

    return session


def _pd_pick_proxy() -> Tuple[Optional[dict], Optional[dict]]:
    """Выбрать прокси из менеджера (или None для прямого соединения)."""
    available = proxy_manager.get_available_proxies()
    if proxy_manager.active_proxy_id:
        active = next((p for p in available if p["id"] == proxy_manager.active_proxy_id), None)
        if active:
            return active, proxy_manager.build_requests_dict(active)
    if not available:
        return None, None
    import random
    p = random.choice(available)
    return p, proxy_manager.build_requests_dict(p)


def get_file_info(file_id: str) -> PixeldrainFile:
    """
    Запросить метаданные файла через GET /api/file/{id}/info.
    Возвращает PixeldrainFile.
    """
    url = f"{PIXELDRAIN_API}/file/{file_id}/info"
    for attempt in range(5):
        try:
            _, req_proxies = _pd_pick_proxy()
            session = _pd_session(req_proxies)
            resp = session.get(url, timeout=(10, 30))
            if resp.status_code == 200:
                data = resp.json()
                return PixeldrainFile(
                    file_id=data["id"],
                    name=data.get("name", file_id),
                    size=int(data.get("size", 0)),
                )
            elif resp.status_code == 404:
                raise RuntimeError(f"Файл Pixeldrain '{file_id}' не найден (404).")
            else:
                raise RuntimeError(f"Pixeldrain API ошибка {resp.status_code}: {resp.text[:200]}")
        except RuntimeError:
            raise
        except Exception as e:
            if attempt < 4:
                time.sleep(1 + attempt)
            else:
                raise RuntimeError(f"Ошибка получения метаданных файла '{file_id}': {e}") from e
    raise RuntimeError(f"Не удалось получить метаданные файла '{file_id}'.")


def get_list_info(list_id: str) -> PixeldrainList:
    """
    Запросить метаданные коллекции через GET /api/list/{id}.
    Возвращает PixeldrainList со списком файлов.
    """
    url = f"{PIXELDRAIN_API}/list/{list_id}"
    for attempt in range(5):
        try:
            _, req_proxies = _pd_pick_proxy()
            session = _pd_session(req_proxies)
            resp = session.get(url, timeout=(10, 30))
            if resp.status_code == 200:
                data = resp.json()
                files = []
                for entry in data.get("files", []):
                    files.append(PixeldrainFile(
                        file_id=entry.get("id", ""),
                        name=entry.get("name", entry.get("id", "")),
                        size=int(entry.get("size", 0)),
                    ))
                return PixeldrainList(
                    list_id=data.get("id", list_id),
                    title=data.get("title") or f"Pixeldrain List {list_id}",
                    files=files,
                )
            elif resp.status_code == 404:
                raise RuntimeError(f"Список Pixeldrain '{list_id}' не найден (404).")
            else:
                raise RuntimeError(f"Pixeldrain API ошибка {resp.status_code}: {resp.text[:200]}")
        except RuntimeError:
            raise
        except Exception as e:
            if attempt < 4:
                time.sleep(1 + attempt)
            else:
                raise RuntimeError(f"Ошибка получения метаданных списка '{list_id}': {e}") from e
    raise RuntimeError(f"Не удалось получить метаданные списка '{list_id}'.")


# ── Инспектор структуры ────────────────────────────────────────────────────────

def inspect_pixeldrain_list(url: str) -> dict:
    """
    Получить дерево файлов и адаптивные сегменты списка Pixeldrain.
    Возвращает dict в том же формате, что и api.inspect_folder_tree() для MEGA:
      {
        "folder_name": str,
        "total_bytes": int,
        "total_files": int,
        "tree": [...],
        "segments": [...]
      }
    """
    from .worker import build_adaptive_batches

    parsed = parse_pixeldrain_url(url)
    if parsed["type"] != "list":
        raise ValueError("inspect_pixeldrain_list поддерживает только ссылки на списки (/l/).")

    pd_list = get_list_info(parsed["id"])
    add_log(f"📂 Pixeldrain список: «{pd_list.title}» ({len(pd_list.files)} файлов, {format_bytes(pd_list.total_bytes)})", "INFO")

    # Строим плоское дерево (списки Pixeldrain не имеют вложенных папок)
    tree_nodes = []
    for f in pd_list.files:
        tree_nodes.append({
            "name": f.name,
            "type": "file",
            "path": f.file_id,   # path = file_id, используется как ключ выбора
            "size": f.size,
        })
    tree_nodes.sort(key=lambda x: x["name"].lower())

    # Адаптивные сегменты через существующий build_adaptive_batches
    # Pixeldrain-файлы представляем через dummy-объекты с .rel_path и .file_size
    class _FakeItem:
        def __init__(self, file_id: str, name: str, size: int):
            self.rel_path = file_id
            self.file_name = name
            self.file_size = size

    fake_items = [_FakeItem(f.file_id, f.name, f.size) for f in pd_list.files]
    batches = build_adaptive_batches(fake_items)
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
        "folder_name": sanitize_filename(pd_list.title),
        "total_bytes": pd_list.total_bytes,
        "total_files": len(pd_list.files),
        "tree": tree_nodes,
        "segments": segments,
        # Дополнительно для worker'а
        "_pixeldrain_list_id": pd_list.list_id,
    }


# ── Прогресс-трекер ────────────────────────────────────────────────────────────

class PixeldrainProgressTracker:
    """Потокобезопасный трекер прогресса и скорости скачивания."""

    def __init__(self, total_bytes: int, task_id: Optional[str] = None):
        self.total_bytes = total_bytes
        self.task_id = task_id
        self.downloaded_bytes = 0
        self.session_downloaded_bytes = 0
        self.completed_files = 0
        self.total_files = 1
        self.start_time = time.time()
        self.last_update_time = 0.0
        self._lock = threading.Lock()

    def add_bytes(self, num_bytes: int) -> None:
        with self._lock:
            self.downloaded_bytes += num_bytes
            self.session_downloaded_bytes += num_bytes
            now = time.time()
            if now - self.last_update_time < 0.6:
                return
            self.last_update_time = now

            elapsed = max(0.1, now - self.start_time)
            speed = self.session_downloaded_bytes / elapsed
            pct = (self.downloaded_bytes / self.total_bytes * 100) if self.total_bytes > 0 else 0
            pct = min(100.0, max(0.0, pct))
            speed_str = f"{format_bytes(int(speed))}/s"
            prog_str = f"{format_bytes(self.downloaded_bytes)} / {format_bytes(self.total_bytes)}"
            file_info = f"[{self.completed_files}/{self.total_files} файлов] " if self.total_files > 1 else ""
            msg = f"⏳ Скачивание (Pixeldrain): {file_info}{pct:.1f}% ({prog_str}) @ {speed_str}"
            update_state(progress=round(pct, 1), speed=speed_str, message=msg)
            if self.task_id:
                update_task(self.task_id, progress=round(pct, 1), speed=speed_str)

    def file_finished(self) -> None:
        with self._lock:
            self.completed_files += 1


# ── Загрузчик файлов ───────────────────────────────────────────────────────────

def _get_sidecar_path(part_path: Path) -> Path:
    return part_path.with_name(part_path.name + ".progress.json")


def _load_chunk_progress(part_path: Path, file_size: int) -> Set[int]:
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


def _save_chunk_progress(part_path: Path, file_size: int, done_starts: Set[int]) -> None:
    sidecar = _get_sidecar_path(part_path)
    tmp = sidecar.with_suffix(".tmp")
    try:
        payload = {"file_size": file_size, "done_starts": sorted(done_starts)}
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, sidecar)
    except Exception:
        pass


def _split_chunks(file_size: int, chunk_size: int = PD_CHUNK_SIZE) -> List[Tuple[int, int]]:
    """Разбить файл на чанки [start, end]."""
    if file_size <= 0:
        return []
    chunks = []
    start = 0
    while start < file_size:
        end = min(start + chunk_size - 1, file_size - 1)
        chunks.append((start, end))
        start = end + 1
    return chunks


class PixeldrainFileDownloader:
    """
    Многопоточный загрузчик одного файла с Pixeldrain.
    Использует Range-запросы, sidecar-файлы прогресса и ротацию прокси.
    """

    def __init__(
        self,
        tracker: PixeldrainProgressTracker,
        concurrency: int = PD_PARALLEL_CHUNK_WORKERS,
    ):
        self.tracker = tracker
        self.concurrency = max(1, concurrency)
        self._file_lock = threading.Lock()
        self._proxy_index = 0
        self._proxy_idx_lock = threading.Lock()
        self._direct_quota_hit = False

    def _pick_proxy(self) -> Tuple[Optional[dict], Optional[dict]]:
        available = proxy_manager.get_available_proxies()
        if proxy_manager.active_proxy_id:
            active = next((p for p in available if p["id"] == proxy_manager.active_proxy_id), None)
            if active:
                return active, proxy_manager.build_requests_dict(active)

        if not self._direct_quota_hit and not available:
            return None, None

        if not available:
            if proxy_manager.auto_rotate:
                available = proxy_manager.ensure_working_proxies(min_count=2, target_count=35)
            if not available:
                return None, None

        with self._proxy_idx_lock:
            self._proxy_index = (self._proxy_index + 1) % len(available)
            p = available[self._proxy_index]
            return p, proxy_manager.build_requests_dict(p)

    def download(self, pd_file: PixeldrainFile, output_path: Path) -> Path:
        """Скачать pd_file в output_path. Возвращает путь к готовому файлу."""
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = output_path.with_suffix(output_path.suffix + ".part")
        cdn_url = PIXELDRAIN_CDN.format(file_id=pd_file.file_id)

        if pd_file.size == 0:
            output_path.write_bytes(b"")
            self.tracker.file_finished()
            return output_path

        # Подготовка .part-файла
        if not part_path.exists() or not _get_sidecar_path(part_path).exists():
            with open(part_path, "wb") as f:
                f.truncate(pd_file.size)
            done_starts: Set[int] = set()
        else:
            done_starts = _load_chunk_progress(part_path, pd_file.size)

        all_chunks = _split_chunks(pd_file.size, PD_CHUNK_SIZE)
        pending_chunks = [c for c in all_chunks if c[0] not in done_starts]

        # Учесть уже скачанные чанки
        initial_bytes = sum((end - start + 1) for start, end in all_chunks if start in done_starts)
        if initial_bytes > 0:
            self.tracker.add_bytes(initial_bytes)

        if len(pending_chunks) <= 1:
            for start, end in pending_chunks:
                self._download_chunk(cdn_url, pd_file, part_path, start, end)
                done_starts.add(start)
                _save_chunk_progress(part_path, pd_file.size, done_starts)
        else:
            with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
                futures = {
                    executor.submit(self._download_chunk, cdn_url, pd_file, part_path, start, end): (start, end)
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
                        _save_chunk_progress(part_path, pd_file.size, done_starts)
                    except Exception as e:
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise RuntimeError(f"Ошибка скачивания блока {start}-{end}: {e}") from e

        # Завершение: убрать .progress.json и переименовать .part → готовый файл
        _get_sidecar_path(part_path).unlink(missing_ok=True)
        if output_path.exists():
            output_path.unlink()
        os.replace(part_path, output_path)

        self.tracker.file_finished()
        return output_path

    def _download_chunk(
        self,
        cdn_url: str,
        pd_file: PixeldrainFile,
        part_path: Path,
        start: int,
        end: int,
    ) -> None:
        """Скачать один байтовый диапазон и записать в .part-файл."""
        headers = {"Range": f"bytes={start}-{end}"}
        chunk_len = end - start + 1

        for attempt in range(PD_MAX_CHUNK_RETRIES):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            proxy_info, req_proxies = self._pick_proxy()
            proxy_name = (
                f"{proxy_info['host']}:{proxy_info['port']}"
                if proxy_info
                else "Прямое соединение"
            )

            try:
                session = PixeldrainSessionPool.get_session(req_proxies)
                with session.get(
                    cdn_url,
                    headers=headers,
                    stream=True,
                    timeout=(10, 60),
                ) as resp:

                    # 429 / 509 — превышение лимита по IP или пропускной способности
                    if resp.status_code in (429, 509):
                        add_log(
                            f"⚠️ Pixeldrain: лимит на {proxy_name} (HTTP {resp.status_code}). Ротация прокси...",
                            "WARNING",
                        )
                        if proxy_info:
                            proxy_manager.mark_proxy_quota(proxy_info["id"], f"Pixeldrain лимит (HTTP {resp.status_code})")
                            if not proxy_manager.get_available_proxies() and proxy_manager.auto_rotate:
                                proxy_manager.ensure_working_proxies(min_count=2, target_count=35)
                        else:
                            self._direct_quota_hit = True
                            if proxy_manager.auto_rotate:
                                proxy_manager.ensure_working_proxies(min_count=2, target_count=35)
                        time.sleep(2)
                        continue

                    # 403 — лимит (нужна captcha) или другие ограничения
                    if resp.status_code == 403:
                        try:
                            err_data = resp.json()
                            err_value = err_data.get("value", "")
                            err_msg = err_data.get("message", "")
                        except Exception:
                            err_value = ""
                            err_msg = ""

                        # 1. Лимит конкретного файла на сервере Pixeldrain
                        # Смена прокси не поможет, файл закрыт для анонимных скачиваний.
                        # НЕ штрафуем прокси!
                        if err_value in _PIXELDRAIN_FILE_LIMIT_VALUES:
                            raise PixeldrainFileRateLimitedError(
                                f"Файл «{pd_file.name}» заблокирован Pixeldrain ({err_value}). "
                                f"Суточный лимит файла (требуется капча на сайте или Pixeldrain API Key)."
                            )

                        # 2. Лимит по IP/прокси (ротация)
                        if err_value in _PIXELDRAIN_IP_LIMIT_VALUES:
                            add_log(
                                f"⚠️ Pixeldrain: лимит скачивания на {proxy_name} ({err_value}). Ротация прокси...",
                                "WARNING",
                            )
                            if proxy_info:
                                proxy_manager.mark_proxy_quota(proxy_info["id"], f"Pixeldrain лимит: {err_value}")
                                if not proxy_manager.get_available_proxies() and proxy_manager.auto_rotate:
                                    proxy_manager.ensure_working_proxies(min_count=2, target_count=35)
                            else:
                                self._direct_quota_hit = True
                                if proxy_manager.auto_rotate:
                                    proxy_manager.ensure_working_proxies(min_count=2, target_count=35)
                            time.sleep(2)
                            continue

                        raise RuntimeError(f"Pixeldrain: HTTP 403 ({err_value or err_msg or 'запрещено'})")

                    if resp.status_code not in (200, 206):
                        raise RuntimeError(f"Pixeldrain: HTTP {resp.status_code}")

                    # Читаем и пишем в .part-файл (буфер 1 MB для максимальной пропускной способности)
                    buf = bytearray()
                    for chunk_bytes in resp.iter_content(chunk_size=1048576):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")
                        if chunk_bytes:
                            buf.extend(chunk_bytes)
                            self.tracker.add_bytes(len(chunk_bytes))

                    if len(buf) != chunk_len:
                        raise RuntimeError(
                            f"Неполный блок: получено {len(buf)} из {chunk_len} байт"
                        )

                    with self._file_lock:
                        with open(part_path, "r+b") as f:
                            f.seek(start)
                            f.write(buf)
                    return

            except PixeldrainFileRateLimitedError:
                # Ни в коем случае не трогаем статус прокси! Прокси полностью рабочий.
                raise
            except Exception as e:
                if stop_event.is_set():
                    raise RuntimeError("Остановлено пользователем")
                if proxy_info:
                    proxy_manager.mark_proxy_offline(proxy_info["id"], str(e)[:60])
                if attempt < PD_MAX_CHUNK_RETRIES - 1:
                    time.sleep(1 + attempt)

        raise RuntimeError(
            f"Не удалось скачать блок {start}-{end} после {PD_MAX_CHUNK_RETRIES} попыток"
        )


# ── Высокоуровневые функции скачивания ────────────────────────────────────────

def download_pixeldrain_file(
    pd_file: PixeldrainFile,
    output_path: Path,
    tracker: PixeldrainProgressTracker,
    concurrency: Optional[int] = None,
) -> Path:
    """Скачать один файл Pixeldrain в output_path."""
    if concurrency is None:
        concurrency = get_pixeldrain_chunk_workers()
    downloader = PixeldrainFileDownloader(tracker, concurrency=concurrency)
    return downloader.download(pd_file, output_path)


def download_pixeldrain_list_items(
    files: List[PixeldrainFile],
    dest_dir: Path,
    tracker: PixeldrainProgressTracker,
    concurrency: Optional[int] = None,
) -> List[Path]:
    """
    Скачать несколько файлов из списка Pixeldrain в dest_dir.
    Загрузка параллельная (до concurrency файлов одновременно).
    Возвращает список путей скачанных файлов.
    """
    if not files:
        return []

    if concurrency is None:
        concurrency = get_pixeldrain_concurrency()

    # Сколько чанков скачивать параллельно для каждого файла (премиум = больше)
    file_chunk_concurrency = 4 if get_pixeldrain_api_key() else 1

    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: List[Path] = []

    def _download_one(pd_file: PixeldrainFile) -> Optional[Path]:
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")
        out = dest_dir / sanitize_filename(pd_file.name)
        # При коллизии имён добавляем file_id
        if out.exists() and any(f.name == pd_file.name for f in files if f is not pd_file):
            stem = out.stem
            suffix = out.suffix
            out = out.with_name(f"{stem}_{pd_file.file_id}{suffix}")
        # Каждый файл скачивается в отдельном экземпляре PixeldrainFileDownloader
        # (не совместный), чтобы исключить гонку за статус прокси
        file_downloader = PixeldrainFileDownloader(tracker, concurrency=file_chunk_concurrency)
        for file_attempt in range(3):
            try:
                return file_downloader.download(pd_file, out)
            except PixeldrainFileRateLimitedError:
                # Если файл заблокирован Pixeldrain — ретраить бесполезно, очищаем временные файлы
                part = out.with_suffix(out.suffix + ".part")
                part.unlink(missing_ok=True)
                _get_sidecar_path(part).unlink(missing_ok=True)
                raise
            except Exception as e:
                if stop_event.is_set():
                    raise
                if file_attempt == 2:
                    raise RuntimeError(f"Ошибка скачивания '{pd_file.name}': {e}") from e
                time.sleep(1 + file_attempt)

    workers_count = min(concurrency, max(1, len(files)))
    skipped_files: List[PixeldrainFile] = []
    with ThreadPoolExecutor(max_workers=workers_count) as executor:
        futures = {executor.submit(_download_one, f): f for f in files}
        for future in as_completed(futures):
            if stop_event.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                raise RuntimeError("Остановлено пользователем")
            pd_file = futures[future]
            try:
                result = future.result()
                if result:
                    downloaded.append(result)
            except PixeldrainFileRateLimitedError as e:
                skipped_files.append(pd_file)
                add_log(
                    f"⚠️ Pixeldrain: файл «{pd_file.name}» заблокирован сервисом "
                    f"(file_rate_limited_captcha_required: суточный лимит файла на сервере). Пропуск файла...",
                    "WARNING",
                )
                tracker.file_finished()
                tracker.add_bytes(pd_file.size)
            except Exception as e:
                executor.shutdown(wait=False, cancel_futures=True)
                raise RuntimeError(f"Ошибка скачивания '{pd_file.name}': {e}") from e

    if skipped_files:
        add_log(
            f"ℹ️ В списке Pixeldrain пропущено {len(skipped_files)} файлов из-за ограничений Pixeldrain. "
            f"Успешно скачано: {len(downloaded)} файлов.",
            "INFO",
        )

    return downloaded
