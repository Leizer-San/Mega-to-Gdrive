"""
worker.py — Обработчик задач и управление очередью.

process_task() выполняет полный цикл: скачать из MEGA → упаковать → залить на Drive.
worker() — основной цикл, который запускается в фоновом потоке.
"""
import shutil
import traceback

from .config import DOWNLOAD_DIR, MAX_RETRIES, RESERVE_BYTES
from .drive import drive_about, ensure_drive_folder, upload_file
from .helpers import add_log, format_bytes, update_state
from .mega import all_files, apply_zip_mode, build_drive_tree, local_tree_stats, mega_download
from .state import (
    STATE, get_next_task, lock, stop_event, update_task,
)


def process_task(task: dict) -> None:
    """
    Выполнить одну задачу импорта:
      1. Скачать из MEGA в локальную временную папку
      2. Применить zip_mode (если нужно)
      3. Проверить квоту Drive
      4. Зеркалировать структуру папок в Drive
      5. Загрузить все файлы на Drive
    """
    tid          = task["id"]
    url          = task["url"]
    destination_id = task["destination_id"]
    zip_mode     = task.get("zip_mode", "none")
    task_dir     = DOWNLOAD_DIR / tid

    if task_dir.exists():
        shutil.rmtree(task_dir, ignore_errors=True)
    task_dir.mkdir(parents=True, exist_ok=True)

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
        mega_download(url, task_dir)

        # ── 2. ZIP-упаковка ──────────────────────────────────────────────────
        apply_zip_mode(task_dir, zip_mode)

        # ── 3. Проверяем что файлы реально скачались ─────────────────────────
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
            local_root   = entries[0]
            root_drive_id = ensure_drive_folder(local_root.name, destination_id)
        else:
            local_root   = task_dir
            root_drive_id = destination_id

        # ── 6. Зеркалируем структуру папок и грузим файлы ───────────────────
        folder_map = build_drive_tree(local_root, root_drive_id)
        files      = list(all_files(local_root))
        done_bytes = 0

        for idx, file_path in enumerate(files, 1):
            if stop_event.is_set():
                raise RuntimeError("Остановлено пользователем")

            rel       = file_path.relative_to(local_root)
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

    except Exception as exc:
        err       = str(exc)
        retries   = int(task.get("retries", 0)) + 1
        next_status = (
            "retry" if retries < MAX_RETRIES and not stop_event.is_set()
            else "error"
        )
        update_task(tid, status=next_status, retries=retries, error=err)
        update_state(error=err, message="Ошибка импорта")
        add_log(f"Критическая ошибка: {err}", "ERROR")
        add_log(traceback.format_exc(), "TRACE")

    finally:
        shutil.rmtree(task_dir, ignore_errors=True)


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
