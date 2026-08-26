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
from .image_compressor import compress_images_in_directory
from .mega import (
    all_files, apply_zip_mode, build_drive_tree,
    local_tree_stats, mega_get,
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
    Выполнить задачу импорта:
    - Для папок: поэтапная обработка по подпапкам с немедленной очисткой диска Colab
      и персистентным сохранением прогресса на Google Drive.
    - Для файлов: прямое скачивание и загрузка.
    """
    from .mega_api import MegaApiClient, parse_mega_url
    from .native_downloader import (
        PARALLEL_CHUNK_WORKERS,
        NativeFileDownloader,
        ProgressTracker,
        download_folder_batch_items,
    )
    from .mega import sanitize_filename, zip_directory

    tid            = task["id"]
    url            = task["url"]
    destination_id = task["destination_id"]
    zip_mode       = task.get("zip_mode", "none")
    selected_paths = task.get("selected_paths") or []
    task_dir       = DOWNLOAD_DIR / tid
    task_dir.mkdir(parents=True, exist_ok=True)

    proxy_manager.reset_rotation_counter()

    try:
        parsed = parse_mega_url(url)
        link_type = parsed["type"]
        api = MegaApiClient()

        if link_type == "folder":
            # ══════════════════════════════════════════════════════════════════
            # Поэтапная обработка папки (Phased / Segmented Processing)
            # ══════════════════════════════════════════════════════════════════
            update_state(current_task=tid, message="Получение структуры папки MEGA...")
            resolved_folder = api.resolve_folder(parsed["folder_id"], parsed["key"])

            # Фильтрация выбранных путей (если заданы пользователем)
            items = resolved_folder.items
            if selected_paths:
                items = [
                    it for it in items
                    if any(it.rel_path == sp or it.rel_path.startswith(sp.rstrip("/") + "/") for sp in selected_paths)
                ]
                add_log(f"🔍 Применён выбор: {len(items)} из {len(resolved_folder.items)} файлов", "INFO")

            if not items:
                raise RuntimeError("Не выбрано ни одного файла для скачивания.")

            total_folder_bytes = sum(it.file_size for it in items)
            update_task(tid, name=resolved_folder.folder_name, bytes_total=total_folder_bytes)

            # Проверка квоты Drive на общий объем
            quota = drive_about()
            if quota["free"] is not None and total_folder_bytes + RESERVE_BYTES > quota["free"]:
                raise RuntimeError(
                    f"Недостаточно места на Google Диске: нужно {format_bytes(total_folder_bytes)}, "
                    f"свободно {format_bytes(quota['free'])}."
                )

            # Группировка элементов по сегментам (подпапкам)
            batches: dict[str, list] = {}
            for it in items:
                parts = it.rel_path.split("/")
                batch_key = parts[0] if len(parts) > 1 else "_root_files"
                if batch_key not in batches:
                    batches[batch_key] = []
                batches[batch_key].append(it)

            completed_batches = set(task.get("completed_batches") or [])
            done_bytes_accumulated = int(task.get("bytes_done") or 0)

            # Определение корневой папки на Google Drive
            if zip_mode in ("subfolders", "none"):
                root_drive_id = ensure_drive_folder(resolved_folder.folder_name, destination_id)
            else:
                root_drive_id = destination_id

            tracker = ProgressTracker(total_folder_bytes, task_id=tid)
            tracker.total_files = len(items)
            tracker.downloaded_bytes = done_bytes_accumulated

            for batch_idx, (batch_name, batch_items) in enumerate(batches.items(), 1):
                if stop_event.is_set():
                    raise RuntimeError("Остановлено пользователем")

                if batch_name in completed_batches:
                    add_log(f"⏩ Пропуск уже выгруженного сегмента: «{batch_name}»", "INFO")
                    continue

                batch_bytes = sum(it.file_size for it in batch_items)
                display_name = resolved_folder.folder_name if batch_name == "_root_files" else batch_name
                add_log(
                    f"📦 Обработка сегмента [{batch_idx}/{len(batches)}]: «{display_name}» "
                    f"({len(batch_items)} файлов, {format_bytes(batch_bytes)})",
                    "INFO",
                )

                # 1. Скачиваем файлы ТОЛЬКО этого сегмента
                update_task(tid, status="downloading")
                folder_root = task_dir / resolved_folder.folder_name
                folder_root.mkdir(parents=True, exist_ok=True)

                download_folder_batch_items(
                    batch_items,
                    folder_id=parsed["folder_id"],
                    folder_root=folder_root,
                    tracker=tracker,
                    api=api,
                )

                # 2. Сжатие изображений в сегменте (если включено)
                if task.get("compress_images"):
                    update_state(message=f"🖼️ Сжатие изображений в сегменте «{display_name}»...")
                    target_comp_dir = folder_root / batch_name if batch_name != "_root_files" else folder_root
                    compress_images_in_directory(target_comp_dir)

                # 3. ZIP-упаковка сегмента
                update_task(tid, status="uploading")
                upload_items: list[Path] = []
                upload_parent_id = root_drive_id

                if zip_mode == "subfolders" and batch_name != "_root_files":
                    sub_dir = folder_root / batch_name
                    if sub_dir.exists() and sub_dir.is_dir():
                        zip_name = sanitize_filename(batch_name) + ".zip"
                        tmp_zip = task_dir / zip_name
                        zip_directory(sub_dir, tmp_zip)
                        shutil.rmtree(sub_dir, ignore_errors=True)
                        upload_items = [tmp_zip]
                        upload_parent_id = root_drive_id
                    else:
                        upload_items = list(all_files(folder_root))
                        upload_parent_id = root_drive_id
                elif zip_mode == "root" and len(batches) == 1:
                    zip_name = sanitize_filename(resolved_folder.folder_name) + ".zip"
                    tmp_zip = task_dir / zip_name
                    zip_directory(folder_root, tmp_zip)
                    shutil.rmtree(folder_root, ignore_errors=True)
                    upload_items = [tmp_zip]
                    upload_parent_id = destination_id
                else:
                    if batch_name != "_root_files":
                        sub_drive_id = ensure_drive_folder(batch_name, root_drive_id)
                        sub_dir = folder_root / batch_name
                        upload_parent_id = sub_drive_id
                        upload_items = list(all_files(sub_dir)) if sub_dir.exists() else []
                    else:
                        upload_parent_id = root_drive_id
                        upload_items = [p for p in folder_root.iterdir() if p.is_file()]

                # 4. Выгрузка файлов сегмента на Google Drive
                for idx, f_path in enumerate(upload_items, 1):
                    if stop_event.is_set():
                        raise RuntimeError("Остановлено пользователем")

                    f_size = f_path.stat().st_size if f_path.exists() else 0
                    update_state(
                        current_file=f_path.name,
                        message=f"📤 Загрузка в Drive ({idx}/{len(upload_items)}): {f_path.name}...",
                    )
                    upload_file(f_path, upload_parent_id, tid, total_folder_bytes, done_bytes_accumulated)
                    done_bytes_accumulated += f_size

                # 5. Сохраняем прогресс сегмента на Google Диске
                completed_batches.add(batch_name)
                pct = (done_bytes_accumulated / total_folder_bytes * 100) if total_folder_bytes > 0 else 100.0
                update_task(
                    tid,
                    completed_batches=sorted(list(completed_batches)),
                    bytes_done=done_bytes_accumulated,
                    progress=min(100.0, round(pct, 1)),
                )

                # 6. МГНОВЕННАЯ ОЧИСТКА ДИСКА COLAB ПОСЛЕ ВЫГРУЗКИ СЕГМЕНТА
                if batch_name != "_root_files":
                    shutil.rmtree(folder_root / batch_name, ignore_errors=True)
                for f_path in upload_items:
                    f_path.unlink(missing_ok=True)

                add_log(f"✅ Сегмент «{display_name}» выгружен на Google Drive (диск Colab очищен)", "OK")

            # Завершение всей папки
            update_task(tid, status="done", progress=100, bytes_done=total_folder_bytes)
            update_state(message="Импорт завершён", overall_progress=100, current_file=None)
            add_log("Импорт папки завершён успешно", "SUCCESS")
            shutil.rmtree(task_dir, ignore_errors=True)

        else:
            # ══════════════════════════════════════════════════════════════════
            # Обработка одиночного файла
            # ══════════════════════════════════════════════════════════════════
            update_task(tid, status="downloading", error=None)
            update_state(current_task=tid, message="Скачивание файла из MEGA…", error=None)

            mega_get(url, task_dir, task_id=tid)

            if task.get("compress_images"):
                update_state(message="🖼️ Сжатие и оптимизация изображений...")
                compress_images_in_directory(task_dir)

            apply_zip_mode(task_dir, zip_mode)

            count, total = local_tree_stats(task_dir)
            if count == 0:
                raise RuntimeError("MEGA-ссылка не вернула файлов.")
            add_log(f"Подготовка завершена: {count} файлов, {format_bytes(total)}", "OK")

            quota = drive_about()
            if quota["free"] is not None and total + RESERVE_BYTES > quota["free"]:
                raise RuntimeError(
                    f"Недостаточно места: нужно {format_bytes(total)}, "
                    f"свободно {format_bytes(quota['free'])}."
                )

            update_task(tid, status="uploading", bytes_total=total, bytes_done=0)

            entries = list(task_dir.iterdir())
            local_root = entries[0] if (len(entries) == 1 and entries[0].is_dir()) else task_dir
            root_drive_id = ensure_drive_folder(local_root.name, destination_id) if local_root.is_dir() and local_root != task_dir else destination_id

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
            update_state(message="Импорт завершён", overall_progress=100, current_file=None)
            add_log("Импорт завершён успешно", "SUCCESS")
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
