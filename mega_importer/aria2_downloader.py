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
ARIA2_CONNECTIONS_PER_FILE = 16
# Максимальное количество файлов, скачиваемых одновременно
ARIA2_MAX_PARALLEL_DOWNLOADS = 5
ARIA2_CONNECT_TIMEOUT = 30
ARIA2_TIMEOUT = 120

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


def _build_aria2c_cmd(
    url: str,
    output_path: Path,
    api_key: str,
    connections: int = ARIA2_CONNECTIONS_PER_FILE,
    proxy: Optional[str] = None,
) -> List[str]:
    """Собрать команду aria2c для скачивания одного файла."""
    cmd = [
        "aria2c",
        "--http-user=",                  # пустой username (Pixeldrain API)
        f"--http-passwd={api_key}",      # API-ключ как пароль (Basic Auth)
        f"-x{connections}",              # макс. соединений к серверу
        f"-s{connections}",              # разбить файл на N сегментов
        "-c",                            # продолжить прерванную загрузку
        "--auto-file-renaming=false",
        f"--connect-timeout={ARIA2_CONNECT_TIMEOUT}",
        f"--timeout={ARIA2_TIMEOUT}",
        "--retry-wait=15",               # 15 сек между попытками (временные ошибки CDN)
        "--max-tries=20",                # до 20 попыток на файл
        "-d", str(output_path.parent),
        "-o", output_path.name,
        "--console-log-level=warn",
        "--summary-interval=0",
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
    Мониторит прогресс через размер файла на диске.
    """
    url = f"https://pixeldrain.com/api/file/{file_id}"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = _build_aria2c_cmd(url, output_path, api_key, connections, proxy)
    aria2_ctrl = output_path.with_suffix(output_path.suffix + ".aria2")

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        last_size = 0
        # Для файлов, которые уже частично скачаны — учитываем начальный размер
        initial_size = output_path.stat().st_size if output_path.exists() else 0
        if initial_size > 0:
            tracker.add_bytes(initial_size)
            last_size = initial_size

        while process.poll() is None:
            if stop_event.is_set():
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                aria2_ctrl.unlink(missing_ok=True)
                raise RuntimeError("Остановлено пользователем")

            if output_path.exists():
                current_size = output_path.stat().st_size
                delta = current_size - last_size
                if delta > 0:
                    tracker.add_bytes(delta)
                    last_size = current_size

            time.sleep(0.3)

        rc = process.returncode
        if rc != 0:
            stderr_bytes = process.stderr.read() if process.stderr else b""
            stderr = stderr_bytes.decode("utf-8", errors="replace") if isinstance(stderr_bytes, bytes) else str(stderr_bytes)
            raise RuntimeError(f"aria2c завершился с кодом {rc}: {stderr[:300]}")

        # Последний замер прогресса
        if output_path.exists():
            final_size = output_path.stat().st_size
            delta = final_size - last_size
            if delta > 0:
                tracker.add_bytes(delta)

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
            except RuntimeError as e:
                if "Остановлено" in str(e):
                    executor.shutdown(wait=False, cancel_futures=True)
                    raise
                add_log(f"⚠️ aria2c: ошибка скачивания '{pd_file.name}': {e}", "WARNING")
                skipped.append(pd_file)
            except Exception as e:
                executor.shutdown(wait=False, cancel_futures=True)
                raise RuntimeError(f"Ошибка скачивания '{pd_file.name}': {e}") from e

    return downloaded, skipped
