"""
worker.py — Обработчик задач и управление очередью.

process_task() выполняет полный цикл: скачать из MEGA → упаковать → залить на Drive.
При исчерпании квоты или сбоях прокси автоматически переключает прокси и продолжает
скачивание с места остановки (файлы на диске сохраняются при ошибке).
"""
import shutil
import time
import traceback

from .config import DOWNLOAD_DIR, MAX_RETRIES, RESERVE_BYTES
from .drive import drive_about, ensure_drive_folder, upload_file
from .helpers import add_log, format_bytes, update_state
from .mega import (
    all_files, apply_zip_mode, build_drive_tree,
    cleanup_downloaded_duplicates, local_tree_stats, mega_get,
)
from .proxy import ensure_megacmd_server_running, proxy_manager
from .state import (
    STATE, get_next_task, lock, stop_event, update_task,
)


# Ключевые слова для детекции исчерпания квоты MEGA
_QUOTA_KEYWORDS = [
    "квота", "quota", "bandwidth", "зависло", "нет прогресса", "limit",
]

# Ключевые слова для детекции сбоев прокси/сети
_PROXY_FAILURE_KEYWORDS = [
    "access denied", "failed to get account", "failed to connect",
    "connection refused", "network error", "код 11", "код 9",
    "could not connect", "timed out", "timeout",
]


def _is_quota_error(text: str) -> bool:
    """Проверить, является ли ошибка исчерпанием квоты MEGA."""
    t = text.lower()
    return any(kw in t for kw in _QUOTA_KEYWORDS)


def _is_proxy_error(text: str) -> bool:
    """Проверить, является ли ошибка сбоем прокси/сети."""
    t = text.lower()
    return any(kw in t for kw in _PROXY_FAILURE_KEYWORDS)


def _is_server_crash(text: str) -> bool:
    """Проверить, упал ли демон mega-cmd-server."""
    t = text.lower()
    return ("mega-cmd-server process seems to have stopped" in t
            or "unable to connect to service" in t)


def process_task(task: dict) -> None:
    """
    Выполнить одну задачу импорта:
      1. Скачать из MEGA (с авторотацией прокси при квоте)
      2. Применить zip_mode
      3. Проверить квоту Drive
      4. Загрузить все файлы на Drive
    """
    tid = task["id"]
    url = task["url"]
    destination_id = task["destination_id"]
    zip_mode = task.get("zip_mode", "none")
    task_dir = DOWNLOAD_DIR / tid

    task_dir.mkdir(parents=True, exist_ok=True)

    # Сбросить счётчик ротаций перед новой задачей
    proxy_manager.reset_rotation_counter()

    # Счётчик попыток прямого подключения (без прокси) при квоте
    direct_quota_attempts = 0
    MAX_DIRECT_QUOTA_ATTEMPTS = 1

    try:
        update_task(tid, status="downloading", error=None)
        update_state(
            current_task=tid,
            current_file=None,
            overall_progress=0,
            message="Скачивание из MEGA…",
            error=None,
        )

        # ── 1. Скачиваем из MEGA ─────────────────────────────────────────────
        while True:
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            try:
                mega_get(url, task_dir, task_id=tid)
                break  # успех
            except Exception as download_err:
                err_msg = str(download_err)

                # A. Пользователь нажал «Остановить»
                if stop_event.is_set() or "остановлено пользователем" in err_msg.lower():
                    raise RuntimeError("Остановлено пользователем")

                # B. Демон MEGAcmd упал — перезапускаем и повторяем
                if _is_server_crash(err_msg):
                    add_log("⚠️ Перезапуск демона mega-cmd-server...", "WARNING")
                    ensure_megacmd_server_running()
                    time.sleep(2)
                    continue

                # C. Квота MEGA или сбой прокси
                is_quota = _is_quota_error(err_msg)
                # Сбой прокси — только если прокси реально был активен
                is_proxy_fail = (
                    proxy_manager.active_proxy_id is not None
                    and _is_proxy_error(err_msg)
                )

                if is_quota or is_proxy_fail:
                    short_err = err_msg.splitlines()[0][:80]
                    reason = "квоты" if is_quota else "сбоя прокси"
                    add_log(f"⚠️ Ошибка {reason}: {short_err}", "WARNING")

                    # Пробуем ротацию если включена
                    if proxy_manager.auto_rotate:
                        rotated = proxy_manager.rotate_on_quota(
                            error_msg=err_msg.splitlines()[0][:50]
                        )
                        if rotated:
                            add_log(
                                "🔄 Прокси переключен. Продолжаю скачивание…",
                                "OK",
                            )
                            time.sleep(2)
                            continue

                    # Ротация не удалась или выключена.
                    # Если прокси был активен — пробуем прямое подключение (1 раз).
                    if proxy_manager.active_proxy_id is not None:
                        proxy_manager.disable_megacmd_proxy(restart=False)
                        add_log("⚠️ Пробую прямое подключение...", "WARNING")
                        time.sleep(2)
                        continue

                    # Прямое подключение тоже не работает (квота на IP Colab).
                    # Даём MAX_DIRECT_QUOTA_ATTEMPTS попыток, затем — ошибка.
                    direct_quota_attempts += 1
                    if direct_quota_attempts <= MAX_DIRECT_QUOTA_ATTEMPTS:
                        add_log(
                            f"⏳ Квота на прямом IP. Попытка {direct_quota_attempts}/{MAX_DIRECT_QUOTA_ATTEMPTS}.",
                            "WARNING",
                        )
                        time.sleep(5)
                        continue

                    # Всё исчерпано — пробрасываем ошибку
                    raise RuntimeError(
                        f"⚠️ Квота MEGA исчерпана на всех прокси и прямом IP. "
                        f"Добавьте новые прокси или дождитесь сброса квоты (~1 час)."
                    )

                # D. Другая ошибка — пробрасываем без ротации
                raise download_err

        # ── 2. Очистка дубликатов (1) от повторных попыток MEGAcmd ───────────
        cleanup_downloaded_duplicates(task_dir)

        # ── 3. ZIP-упаковка ──────────────────────────────────────────────────
        apply_zip_mode(task_dir, zip_mode)

        # ── 4. Проверка скачанных файлов ─────────────────────────────────────
        count, total = local_tree_stats(task_dir)
        if count == 0:
            raise RuntimeError("MEGA-ссылка не вернула файлов.")
        add_log(f"Подготовка завершена: {count} файлов, {format_bytes(total)}", "OK")

        # ── 4. Проверка квоты Drive ──────────────────────────────────────────
        quota = drive_about()
        if quota["free"] is not None and total + RESERVE_BYTES > quota["free"]:
            raise RuntimeError(
                f"Недостаточно места: нужно {format_bytes(total)}, "
                f"свободно {format_bytes(quota['free'])}."
            )

        update_task(tid, status="uploading", bytes_total=total, bytes_done=0)

        # ── 5. Определяем корень для загрузки ────────────────────────────────
        entries = list(task_dir.iterdir())
        if len(entries) == 1 and entries[0].is_dir():
            local_root = entries[0]
            root_drive_id = ensure_drive_folder(local_root.name, destination_id)
        else:
            local_root = task_dir
            root_drive_id = destination_id

        # ── 6. Загрузка файлов на Drive ──────────────────────────────────────
        folder_map = build_drive_tree(local_root, root_drive_id)
        files = list(all_files(local_root))
        done_bytes = 0

        for idx, file_path in enumerate(files, 1):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            rel = file_path.relative_to(local_root)
            parent_id = folder_map.get(rel.parent, root_drive_id)

            update_state(
                current_file=str(rel),
                message=f"📤 Загрузка в Drive ({idx} из {len(files)})...",
            )

            upload_file(file_path, parent_id, tid, total, done_bytes)
            done_bytes += file_path.stat().st_size

        update_task(tid, status="done", progress=100, bytes_done=total)
        update_state(
            message="Импорт завершён",
            overall_progress=100,
            current_file=None,
        )
        add_log("Импорт завершён успешно", "SUCCESS")
        # Удаляем локальные файлы только после успешной загрузки
        shutil.rmtree(task_dir, ignore_errors=True)

    except Exception as exc:
        err = str(exc)
        retries = int(task.get("retries", 0)) + 1
        next_status = (
            "retry" if retries < MAX_RETRIES and not stop_event.is_set()
            else "error"
        )
        update_task(tid, status=next_status, retries=retries, error=err)
        update_state(error=err, message="Ошибка импорта")
        add_log(f"Критическая ошибка: {err}", "ERROR")
        add_log(traceback.format_exc(), "TRACE")
        # task_dir НЕ удаляется — mega-get продолжит с места остановки


def worker() -> None:
    """
    Основной цикл очереди. Запускается в daemon-потоке.
    Обрабатывает задачи одну за другой до исчерпания очереди или stop_event.
    """
    update_state(running=True, error=None)
    stop_event.clear()
    add_log("Очередь запущена")

    try:
        while not stop_event.is_set():
            task = get_next_task()
            if not task:
                break
            update_state(current_task=task["id"])
            process_task(task)
    finally:
        update_state(running=False, current_task=None, current_file=None)
        add_log("Очередь остановлена")
