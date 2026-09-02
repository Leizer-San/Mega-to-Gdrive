"""
aria2_downloader.py — Высокоскоростной загрузчик на базе aria2c для Pixeldrain Premium.

aria2c — нативная утилита на C, которая поддерживает:
- До 16 соединений к одному серверу (-x16 / -s16)
- HTTP Basic Auth для Pixeldrain API ключа
- Продолжение прерванных загрузок (-c)
- Параллельное скачивание нескольких файлов

Используется автоматически если:
- Установлен Pixeldrain API Key (Premium-аккаунт)
- Доступен aria2c (устанавливается автоматически через apt-get в Colab)
"""
from __future__ import annotations

import shutil
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

from .helpers import add_log, sanitize_filename
from .state import stop_event

# Количество параллельных соединений для одного файла (aria2c -x / -s)
ARIA2_CONNECTIONS_PER_FILE = 8          # 8 соед. × N файлов = разумное общее число
# Максимальное количество файлов, скачиваемых одновременно
ARIA2_MAX_PARALLEL_DOWNLOADS = 4        # 4 файла × 8 соед. = 32 соединения всего
ARIA2_CONNECT_TIMEOUT = 30
ARIA2_TIMEOUT = 120
# Макс. время (сек) без прогресса перед предупреждением в лог
ARIA2_STALL_WARN_SECS = 180             # 3 минуты

_aria2c_available: Optional[bool] = None
_aria2c_lock = threading.Lock()


def is_aria2c_available() -> bool:
    """Проверить наличие aria2c, при необходимости установить через apt-get."""
    global _aria2c_available
    with _aria2c_lock:
        if _aria2c_available is not None:
            return _aria2c_available
        if shutil.which("aria2c"):
            _aria2c_available = True
            return True
        # Пытаемся установить через apt-get (Google Colab / Ubuntu)
        try:
            add_log("⚙️ Установка aria2c для ускоренного скачивания Pixeldrain...", "INFO")
            result = subprocess.run(
                ["apt-get", "install", "-y", "-q", "aria2"],
                capture_output=True, text=True, timeout=90,
            )
            if result.returncode == 0 and shutil.which("aria2c"):
                add_log("✅ aria2c установлен. Следующие файлы будут скачаны значительно быстрее.", "OK")
                _aria2c_available = True
                return True
        except Exception as e:
            add_log(f"⚠️ Не удалось установить aria2c: {e}. Используется стандартный загрузчик.", "WARNING")
        _aria2c_available = False
        return False


import queue
import re

# Регулярные выражения для парсинга вывода aria2c summary:
# [#2089b0 400.0KiB/33.2MiB(1%) CN:1 DL:115.7KiB ETA:4m51s]
_ARIA2_SUMMARY_RE = re.compile(
    r"\[#[0-9a-fA-F]+\s+([\d\.]+)(B|KiB|MiB|GiB|TiB|KB|MB|GB)/"
)
_ARIA2_PCT_RE = re.compile(r"\((\d+)%\)")
_UNIT_MULTIPLIERS = {
    "B": 1,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
}


def _build_aria2c_cmd(
    url: str,
    output_path: Path,
    api_key: str,
    connections: int = ARIA2_CONNECTIONS_PER_FILE,
    proxy: Optional[str] = None,
) -> List[str]:
    """Собрать команду aria2c для скачивания одного файла с упреждающей авторизацией."""
    import base64
    # Превентивная Basic-авторизация в заголовке (Pixeldrain API key как пароль):
    # Pixeldrain отдаёт 403 если нет заголовка Authorization, а aria2c по умолчанию ждёт 401 challenge
    auth_b64 = base64.b64encode(f":{api_key}".encode("utf-8")).decode("ascii")

    cmd = [
        "aria2c",
        f"--header=Authorization: Basic {auth_b64}",
        "--header=User-Agent: MegaGdriveImporter/2.0",
        "--http-auth-challenge=false",   # слать заголовок сразу, не дожидаясь ответа сервера
        "--http-user=",                  # пустой username (Pixeldrain API)
        f"--http-passwd={api_key}",      # API-ключ как пароль (Basic Auth)
        f"-x{connections}",              # макс. соединений к серверу
        f"-s{connections}",              # разбить файл на N сегментов
        "-c",                            # продолжить прерванную загрузку
        "--auto-file-renaming=false",
        f"--connect-timeout={ARIA2_CONNECT_TIMEOUT}",
        f"--timeout={ARIA2_TIMEOUT}",
        "--retry-wait=10",               # 10 сек между попытками
        "--max-tries=15",                # до 15 попыток
        "--console-log-level=notice",
        "--summary-interval=1",          # вывод прогресса каждую 1 секунду
        "--show-console-readout=false",  # чистый построчный вывод без \r
        "-d", str(output_path.parent),
        "-o", output_path.name,
    ]
    if proxy:
        cmd.extend([f"--all-proxy={proxy}"])
    cmd.append(url)
    return cmd


def download_with_aria2c(
    file_id: str,
    file_name: str,
    file_size: int,
    output_path: Path,
    api_key: str,
    tracker,
    connections: int = ARIA2_CONNECTIONS_PER_FILE,
    proxy: Optional[str] = None,
) -> Path:
    """
    Скачать один файл Pixeldrain через aria2c.
    Отслеживает реальный сетевой прогресс через разбор stdout aria2c каждую секунду.
    """
    url = f"https://pixeldrain.com/api/file/{file_id}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = _build_aria2c_cmd(url, output_path, api_key, connections, proxy)
    aria2_ctrl = output_path.with_suffix(output_path.suffix + ".aria2")

    # Если файл уже скачан полностью и нет контрольного .aria2 — пропускаем
    if output_path.exists() and not aria2_ctrl.exists() and output_path.stat().st_size == file_size:
        tracker.add_bytes(file_size)
        tracker.file_finished()
        return output_path

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        q: queue.Queue[Optional[str]] = queue.Queue()

        def _reader():
            try:
                for line in iter(process.stdout.readline, ""):
                    q.put(line)
            except Exception:
                pass
            finally:
                q.put(None)

        reader_thread = threading.Thread(target=_reader, daemon=True)
        reader_thread.start()

        last_reported_bytes = 0
        last_progress_time = time.time()
        stall_warned = False
        output_tail: List[str] = []

        while True:
            if stop_event.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                aria2_ctrl.unlink(missing_ok=True)
                raise RuntimeError("Остановлено пользователем")

            try:
                line = q.get(timeout=0.5)
            except queue.Empty:
                if process.poll() is not None:
                    break
                # Предупреждение если прогресс не менялся долго
                if not stall_warned and (time.time() - last_progress_time) > ARIA2_STALL_WARN_SECS:
                    add_log(
                        f"⏳ aria2c: '{file_name}' не имеет прогресса уже 3 минуты. Идёт retry...",
                        "WARNING",
                    )
                    stall_warned = True
                continue

            if line is None:  # EOF
                break

            stripped = line.strip()
            if stripped:
                output_tail.append(stripped)
                if len(output_tail) > 20:
                    output_tail.pop(0)

            # Парсим байты из строки статуса aria2c
            current_done = None
            m = _ARIA2_SUMMARY_RE.search(stripped)
            if m:
                try:
                    val = float(m.group(1))
                    unit = m.group(2)
                    current_done = int(val * _UNIT_MULTIPLIERS.get(unit, 1))
                except Exception:
                    pass
            elif _ARIA2_PCT_RE.search(stripped):
                try:
                    m_pct = _ARIA2_PCT_RE.search(stripped)
                    pct_val = int(m_pct.group(1))
                    current_done = int(file_size * pct_val / 100)
                except Exception:
                    pass

            if current_done is not None and current_done > last_reported_bytes:
                delta = current_done - last_reported_bytes
                tracker.add_bytes(delta)
                last_reported_bytes = current_done
                last_progress_time = time.time()
                stall_warned = False

        rc = process.wait()
        if rc != 0:
            tail_msg = " | ".join(output_tail[-3:]) if output_tail else ""
            raise RuntimeError(f"aria2c завершился с кодом {rc}: {tail_msg[:300]}")

        # Учитываем оставшиеся байты до полного размера файла
        if file_size > last_reported_bytes:
            tracker.add_bytes(file_size - last_reported_bytes)

        tracker.file_finished()
        return output_path

    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"Ошибка aria2c для '{file_name}': {e}") from e


def download_pixeldrain_list_aria2c(
    files,
    dest_dir: Path,
    tracker,
    api_key: str,
    concurrency: int = ARIA2_MAX_PARALLEL_DOWNLOADS,
    connections_per_file: int = ARIA2_CONNECTIONS_PER_FILE,
    proxy: Optional[str] = None,
) -> Tuple[List[Path], List]:
    """
    Скачать список файлов Pixeldrain через aria2c.
    При ошибке любого файла — автоматический fallback на Python-загрузчик (файл не теряется!).
    Возвращает (downloaded_paths, skipped_files).
    """
    if not files:
        return [], []

    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded: List[Path] = []
    skipped: List = []

    def _download_one(pd_file) -> Optional[Path]:
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")

        out = dest_dir / sanitize_filename(pd_file.name)

        # Файл уже скачан полностью — пропускаем
        if out.exists() and out.stat().st_size == pd_file.size:
            tracker.add_bytes(pd_file.size)
            tracker.file_finished()
            return out

        # Коллизия имён
        if any(f.name == pd_file.name for f in files if f is not pd_file):
            out = out.with_name(f"{out.stem}_{pd_file.file_id}{out.suffix}")

        try:
            return download_with_aria2c(
                file_id=pd_file.file_id,
                file_name=pd_file.name,
                file_size=pd_file.size,
                output_path=out,
                api_key=api_key,
                tracker=tracker,
                connections=connections_per_file,
                proxy=proxy,
            )
        except RuntimeError as err:
            if "Остановлено" in str(err):
                raise
            # Fallback на Python-загрузчик (на случай сбоя aria2c)
            add_log(
                f"⚠️ aria2c: сбой '{pd_file.name}' ({err}). Докачиваем через встроенный Python-загрузчик...",
                "WARNING",
            )
            from .pixeldrain import PixeldrainFileDownloader
            py_downloader = PixeldrainFileDownloader(tracker, concurrency=4)
            return py_downloader.download(pd_file, out)

    workers = min(concurrency, max(1, len(files)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
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
            except Exception as e:
                if stop_event.is_set() or "Остановлено" in str(e):
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise RuntimeError("Остановлено пользователем") from e
                add_log(f"❌ Ошибка скачивания '{pd_file.name}': {e}", "ERROR")
                skipped.append(pd_file)

    return downloaded, skipped
