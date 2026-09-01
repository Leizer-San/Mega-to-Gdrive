"""
worker.py — Обработчик задач и управление очередью.

process_task() выполняет полный цикл: скачать из MEGA / Pixeldrain → упаковать → залить на Drive.
Поддерживает оба провайдера:
  - MEGA: адаптивная батчевая загрузка ~6-8 GB с ротацией прокси.
  - Pixeldrain: загрузка одиночных файлов и списков через Range-запросы с ротацией прокси.
"""
from pathlib import Path
import shutil
import time
import traceback

from .config import DOWNLOAD_DIR, MAX_RETRIES, RESERVE_BYTES
from .drive import drive_about, ensure_drive_folder, upload_file
from .helpers import add_log, format_bytes, get_url_provider, update_state
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


def build_adaptive_batches(
    items: list,
    max_batch_size: int = 8 * 1024 * 1024 * 1024,
) -> dict[str, list]:
    """
    Универсальное адаптивное разбиение элементов папки на порции по ~6-8 GB.
    Работает с любой структурой каталогов, любой глубиной вложенности или плоским списком файлов.
    """
    dir_groups: dict[str, list] = {}
    for it in items:
        parent = "/".join(it.rel_path.split("/")[:-1]) if "/" in it.rel_path else "_root"
        if parent not in dir_groups:
            dir_groups[parent] = []
        dir_groups[parent].append(it)

    batches: dict[str, list] = {}
    current_small_batch: list = []
    current_small_size = 0
    small_batch_idx = 1

    for dir_path, dir_items in dir_groups.items():
        dir_size = sum(it.file_size for it in dir_items)

        if dir_size > max_batch_size:
            part_idx = 1
            cur_part_items = []
            cur_part_size = 0
            for it in dir_items:
                if cur_part_items and (cur_part_size + it.file_size > max_batch_size):
                    b_name = f"{dir_path} (часть {part_idx})" if dir_path != "_root" else f"Файлы (часть {part_idx})"
                    batches[b_name] = cur_part_items
                    part_idx += 1
                    cur_part_items = []
                    cur_part_size = 0
                cur_part_items.append(it)
                cur_part_size += it.file_size
            if cur_part_items:
                b_name = f"{dir_path} (часть {part_idx})" if dir_path != "_root" else f"Файлы (часть {part_idx})"
                batches[b_name] = cur_part_items
        elif dir_size >= max_batch_size * 0.35:
            b_name = dir_path if dir_path != "_root" else "Основные файлы"
            batches[b_name] = dir_items
        else:
            current_small_batch.extend(dir_items)
            current_small_size += dir_size
            if current_small_size >= max_batch_size * 0.7:
                batches[f"Группа {small_batch_idx}"] = current_small_batch
                small_batch_idx += 1
                current_small_batch = []
                current_small_size = 0

    if current_small_batch:
        batches[f"Группа {small_batch_idx}"] = current_small_batch

    return batches


def process_task(task: dict) -> None:
    """
    Выполнить задачу импорта:
    - Для папок: универсальная адаптивная обработка порциями по ~6-8 GB
      с немедленной очисткой диска Colab и персистентным сохранением прогресса на Google Drive.
    - Для файлов: прямое скачивание и загрузка.
    """
    from .mega_api import MegaApiClient, parse_mega_url
    from .native_downloader import (
        PARALLEL_CHUNK_WORKERS,
        NativeFileDownloader,
        ProgressTracker,
        download_folder_batch_items,
    )
    from .helpers import sanitize_filename
    from .mega import zip_directory

    tid            = task["id"]
    url            = task["url"]
    destination_id = task["destination_id"]
    zip_mode       = task.get("zip_mode", "none")
    selected_paths = task.get("selected_paths") or []
    task_dir       = DOWNLOAD_DIR / tid
    task_dir.mkdir(parents=True, exist_ok=True)

    # ── Роутинг по провайдеру ─────────────────────────────────────────────────
    provider = get_url_provider(url)
    if provider == "pixeldrain":
        _process_pixeldrain_task(task, task_dir)
        return

    # ── MEGA ──────────────────────────────────────────────────────────────────
    proxy_manager.reset_rotation_counter()

    try:
        parsed = parse_mega_url(url)
        link_type = parsed["type"]
        api = MegaApiClient()

        if link_type == "folder":
            # ══════════════════════════════════════════════════════════════════
            # Поэтапная адаптивная обработка папки (Adaptive Batching)
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

            # Универсальная адаптивная группировка по ~6-8 GB
            batches = build_adaptive_batches(items, max_batch_size=8 * 1024 * 1024 * 1024)

            completed_batches = set(task.get("completed_batches") or [])
            done_bytes_accumulated = int(task.get("bytes_done") or 0)

            # Подсчет уже завершенных файлов и байтов из пропущенных сегментов
            completed_files_count = 0
            completed_bytes_count = 0
            for b_name, b_items in batches.items():
                if b_name in completed_batches:
                    completed_files_count += len(b_items)
                    completed_bytes_count += sum(it.file_size for it in b_items)

            if done_bytes_accumulated < completed_bytes_count:
                done_bytes_accumulated = completed_bytes_count

            # Корневая папка на Google Drive
            root_drive_id = ensure_drive_folder(resolved_folder.folder_name, destination_id)

            # Кэш соответствия относительных путей каталогов к ID на Google Диске
            drive_folder_cache: dict[str, str] = {".": root_drive_id}

            def get_drive_parent_id(rel_dir: Path) -> str:
                s = str(rel_dir).replace("\\", "/")
                if s in drive_folder_cache:
                    return drive_folder_cache[s]
                parts = rel_dir.parts
                curr_id = root_drive_id
                curr_path = Path(".")
                for p in parts:
                    curr_path = curr_path / p
                    cp_str = str(curr_path).replace("\\", "/")
                    if cp_str not in drive_folder_cache:
                        drive_folder_cache[cp_str] = ensure_drive_folder(p, curr_id)
                    curr_id = drive_folder_cache[cp_str]
                return curr_id

            tracker = ProgressTracker(total_folder_bytes, task_id=tid)
            tracker.total_files = len(items)
            tracker.completed_files = completed_files_count
            tracker.downloaded_bytes = done_bytes_accumulated

            for batch_idx, (batch_name, batch_items) in enumerate(batches.items(), 1):
                if stop_event.is_set():
                    raise RuntimeError("Остановлено пользователем")

                if batch_name in completed_batches:
                    add_log(f"⏩ Пропуск уже выгруженного сегмента: «{batch_name}»", "INFO")
                    continue

                batch_bytes = sum(it.file_size for it in batch_items)
                add_log(
                    f"📦 Обработка сегмента [{batch_idx}/{len(batches)}]: «{batch_name}» "
                    f"({len(batch_items)} файлов, {format_bytes(batch_bytes)})",
                    "INFO",
                )

                # 1. Скачиваем файлы ТОЛЬКО этого сегмента в их исходные подпапки
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
                    update_state(message=f"🖼️ Сжатие изображений в сегменте «{batch_name}»...")
                    # Сжимаем только родительские каталоги файлов этого сегмента
                    touched_dirs = {
                        (folder_root / it.rel_path).parent
                        for it in batch_items
                        if (folder_root / it.rel_path).parent.exists()
                    }
                    for td in touched_dirs:
                        compress_images_in_directory(td)

                # 3. Выгрузка файлов сегмента на Google Drive
                update_task(tid, status="uploading")

                if zip_mode == "subfolders":
                    # Упаковка каждой подпапки сегмента в отдельный .zip
                    sub_dirs = [p for p in folder_root.iterdir() if p.is_dir()]
                    for s_idx, s_dir in enumerate(sub_dirs, 1):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")

                        # Если сегмент разбит на части (например, "Folder (часть 1)")
                        if "часть" in batch_name and s_dir.name in batch_name:
                            zip_name = sanitize_filename(batch_name) + ".zip"
                        else:
                            zip_name = sanitize_filename(s_dir.name) + ".zip"

                        tmp_zip = task_dir / zip_name
                        update_state(message=f"📦 Упаковка подпапки ({s_idx}/{len(sub_dirs)}): {zip_name}...")
                        zip_directory(s_dir, tmp_zip)
                        shutil.rmtree(s_dir, ignore_errors=True)

                        f_size = tmp_zip.stat().st_size
                        update_state(
                            current_file=zip_name,
                            message=f"📤 Загрузка в Drive ({s_idx}/{len(sub_dirs)}): {zip_name} ({format_bytes(f_size)})...",
                        )
                        upload_file(tmp_zip, root_drive_id, tid, total_folder_bytes, done_bytes_accumulated)
                        done_bytes_accumulated += f_size
                        tmp_zip.unlink(missing_ok=True)

                    # Файлы, находящиеся непосредственно в корне папки
                    root_files = [p for p in folder_root.iterdir() if p.is_file()]
                    for r_file in root_files:
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")
                        f_size = r_file.stat().st_size
                        update_state(
                            current_file=r_file.name,
                            message=f"📤 Загрузка в Drive: {r_file.name}...",
                        )
                        upload_file(r_file, root_drive_id, tid, total_folder_bytes, done_bytes_accumulated)
                        done_bytes_accumulated += f_size
                        r_file.unlink(missing_ok=True)

                elif zip_mode == "root":
                    # Упаковка всего сегмента в один .zip
                    if len(batches) == 1:
                        zip_name = sanitize_filename(resolved_folder.folder_name) + ".zip"
                        upload_target_id = destination_id
                    else:
                        zip_name = sanitize_filename(f"{resolved_folder.folder_name} - {batch_name}") + ".zip"
                        upload_target_id = root_drive_id

                    tmp_zip = task_dir / zip_name
                    update_state(message=f"📦 Упаковка архива {zip_name}...")
                    zip_directory(folder_root, tmp_zip)
                    shutil.rmtree(folder_root, ignore_errors=True)
                    folder_root.mkdir(parents=True, exist_ok=True)

                    f_size = tmp_zip.stat().st_size
                    update_state(
                        current_file=zip_name,
                        message=f"📤 Загрузка в Drive архива {zip_name} ({format_bytes(f_size)})...",
                    )
                    upload_file(tmp_zip, upload_target_id, tid, total_folder_bytes, done_bytes_accumulated)
                    done_bytes_accumulated += f_size
                    tmp_zip.unlink(missing_ok=True)

                else:
                    # zip_mode == "none": поштучная загрузка с сохранением структуры папок
                    for idx, it in enumerate(batch_items, 1):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")

                        # Файл на диске (мог стать .jpg после сжатия)
                        f_path = folder_root / it.rel_path
                        if not f_path.exists():
                            # Проверяем, был ли конвертирован в .jpg
                            jpg_alt = f_path.with_suffix(".jpg")
                            if jpg_alt.exists():
                                f_path = jpg_alt

                        if not f_path.exists():
                            continue

                        f_size = f_path.stat().st_size
                        rel_p = Path(it.rel_path)
                        parent_drive_id = get_drive_parent_id(rel_p.parent)

                        update_state(
                            current_file=f_path.name,
                            message=f"📤 Загрузка в Drive ({idx}/{len(batch_items)}): {f_path.name}...",
                        )
                        upload_file(f_path, parent_drive_id, tid, total_folder_bytes, done_bytes_accumulated)
                        done_bytes_accumulated += f_size

                # 4. Сохраняем прогресс сегмента на Google Диске
                completed_batches.add(batch_name)
                pct = (done_bytes_accumulated / total_folder_bytes * 100) if total_folder_bytes > 0 else 100.0
                update_task(
                    tid,
                    completed_batches=sorted(list(completed_batches)),
                    bytes_done=done_bytes_accumulated,
                    progress=min(100.0, round(pct, 1)),
                )

                # 5. МГНОВЕННАЯ ОЧИСТКА ДИСКА COLAB ПОСЛЕ ВЫГРУЗКИ СЕГМЕНТА
                for it in batch_items:
                    (folder_root / it.rel_path).unlink(missing_ok=True)
                    (folder_root / it.rel_path).with_suffix(".jpg").unlink(missing_ok=True)

                # Удаление пустых директорий
                for d in sorted([p for p in folder_root.rglob("*") if p.is_dir()], key=lambda x: len(x.parts), reverse=True):
                    try:
                        d.rmdir()
                    except OSError:
                        pass

                add_log(f"✅ Сегмент «{batch_name}» выгружен на Google Drive (диск Colab очищен)", "OK")

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



def _process_pixeldrain_task(task: dict, task_dir: Path) -> None:
    """
    Обработать задачу импорта для Pixeldrain.
    Поддерживает одиночные файлы (/u/ID) и коллекции/списки (/l/ID).
    Реализует полный пайплайн: скачать → сжать → упаковать → залить на Drive.
    """
    from .pixeldrain import (
        parse_pixeldrain_url,
        get_file_info,
        get_list_info,
        PixeldrainFile,
        PixeldrainProgressTracker,
        download_pixeldrain_file,
        download_pixeldrain_list_items,
        PD_PARALLEL_CHUNK_WORKERS,
        PD_PARALLEL_FOLDER_WORKERS,
    )
    from .helpers import sanitize_filename
    from .mega import zip_directory

    tid            = task["id"]
    url            = task["url"]
    destination_id = task["destination_id"]
    zip_mode       = task.get("zip_mode", "none")
    selected_paths = task.get("selected_paths") or []  # для списков: выбранные file_id

    try:
        parsed = parse_pixeldrain_url(url)

        # ══════════════════════════════════════════════════════════════════════
        # ОДИНОЧНЫЙ ФАЙЛ Pixeldrain
        # ══════════════════════════════════════════════════════════════════════
        if parsed["type"] == "file":
            file_id = parsed["id"]
            update_task(tid, status="downloading", error=None)
            update_state(current_task=tid, message="Получение информации о файле Pixeldrain...", error=None)

            pd_file = get_file_info(file_id)
            update_task(tid, name=pd_file.name, bytes_total=pd_file.size)
            add_log(f"📥 Pixeldrain файл: «{pd_file.name}» ({format_bytes(pd_file.size)})", "INFO")

            # Проверка квоты на Google Drive
            quota = drive_about()
            if quota["free"] is not None and pd_file.size + RESERVE_BYTES > quota["free"]:
                raise RuntimeError(
                    f"Недостаточно места на Google Диске: нужно {format_bytes(pd_file.size)}, "
                    f"свободно {format_bytes(quota['free'])}."
                )

            tracker = PixeldrainProgressTracker(pd_file.size, task_id=tid)
            out_path = task_dir / sanitize_filename(pd_file.name)
            download_pixeldrain_file(pd_file, out_path, tracker, concurrency=PD_PARALLEL_CHUNK_WORKERS)
            add_log(f"✅ Файл скачан: {pd_file.name} ({format_bytes(pd_file.size)})", "OK")

            # Сжатие изображений
            if task.get("compress_images"):
                update_state(message="🖼️ Сжатие и оптимизация изображений...")
                compress_images_in_directory(task_dir)

            # ZIP-упаковка
            if zip_mode == "root":
                zip_name = sanitize_filename(pd_file.name) + ".zip"
                tmp_zip = task_dir.parent / zip_name
                update_state(message=f"📦 Упаковка в архив {zip_name}...")
                zip_directory(task_dir, tmp_zip)
                shutil.rmtree(task_dir, ignore_errors=True)
                task_dir.mkdir(parents=True, exist_ok=True)
                f_size = tmp_zip.stat().st_size
                update_task(tid, status="uploading", bytes_total=f_size, bytes_done=0)
                update_state(
                    current_file=zip_name,
                    message=f"📤 Загрузка в Drive: {zip_name} ({format_bytes(f_size)})...",
                )
                upload_file(tmp_zip, destination_id, tid, f_size, 0)
                tmp_zip.unlink(missing_ok=True)
            else:
                update_task(tid, status="uploading", bytes_done=0)
                f_size = out_path.stat().st_size if out_path.exists() else pd_file.size
                update_state(
                    current_file=pd_file.name,
                    message=f"📤 Загрузка в Drive: {pd_file.name} ({format_bytes(f_size)})...",
                )
                upload_file(out_path, destination_id, tid, f_size, 0)

            update_task(tid, status="done", progress=100, bytes_done=pd_file.size)
            update_state(message="Импорт завершён", overall_progress=100, current_file=None)
            add_log("Импорт файла Pixeldrain завершён успешно", "SUCCESS")
            shutil.rmtree(task_dir, ignore_errors=True)

        # ══════════════════════════════════════════════════════════════════════
        # СПИСОК / КОЛЛЕКЦИЯ Pixeldrain
        # ══════════════════════════════════════════════════════════════════════
        elif parsed["type"] == "list":
            list_id = parsed["id"]
            update_state(current_task=tid, message="Получение информации о списке Pixeldrain...")
            pd_list = get_list_info(list_id)

            # Фильтрация выбранных файлов (selected_paths = список file_id)
            items: list[PixeldrainFile] = pd_list.files
            if selected_paths:
                items = [f for f in pd_list.files if f.file_id in selected_paths]
                add_log(
                    f"🔍 Применён выбор: {len(items)} из {len(pd_list.files)} файлов",
                    "INFO",
                )

            if not items:
                raise RuntimeError("Не выбрано ни одного файла для скачивания.")

            total_bytes = sum(f.size for f in items)
            folder_name = sanitize_filename(pd_list.title)
            update_task(tid, name=folder_name, bytes_total=total_bytes)
            add_log(
                f"📂 Pixeldrain список: «{pd_list.title}» ({len(items)} файлов, {format_bytes(total_bytes)})",
                "INFO",
            )

            # Проверка квоты на Google Drive
            quota = drive_about()
            if quota["free"] is not None and total_bytes + RESERVE_BYTES > quota["free"]:
                raise RuntimeError(
                    f"Недостаточно места на Google Диске: нужно {format_bytes(total_bytes)}, "
                    f"свободно {format_bytes(quota['free'])}."
                )

            # Адаптивная группировка по ~6-8 GB сегментам
            class _FakeItem:
                def __init__(self, f: "PixeldrainFile"):
                    self.rel_path = f.file_id
                    self.file_name = f.name
                    self.file_size = f.size
                    self._pd_file = f

            fake_items = [_FakeItem(f) for f in items]
            batches = build_adaptive_batches(fake_items, max_batch_size=8 * 1024 * 1024 * 1024)

            completed_batches = set(task.get("completed_batches") or [])
            done_bytes_accumulated = int(task.get("bytes_done") or 0)

            # Корневая папка на Google Drive
            root_drive_id = ensure_drive_folder(folder_name, destination_id)

            tracker = PixeldrainProgressTracker(total_bytes, task_id=tid)
            tracker.total_files = len(items)
            tracker.downloaded_bytes = done_bytes_accumulated

            folder_root = task_dir / folder_name
            folder_root.mkdir(parents=True, exist_ok=True)

            for batch_idx, (batch_name, batch_fake_items) in enumerate(batches.items(), 1):
                if stop_event.is_set():
                    raise RuntimeError("Остановлено пользователем")

                if batch_name in completed_batches:
                    add_log(f"⏩ Пропуск уже выгруженного сегмента: «{batch_name}»", "INFO")
                    continue

                batch_pd_files = [fi._pd_file for fi in batch_fake_items]
                batch_bytes = sum(f.size for f in batch_pd_files)
                add_log(
                    f"📦 Обработка сегмента [{batch_idx}/{len(batches)}]: «{batch_name}» "
                    f"({len(batch_pd_files)} файлов, {format_bytes(batch_bytes)})",
                    "INFO",
                )

                # 1. Скачивание файлов сегмента
                update_task(tid, status="downloading")
                download_pixeldrain_list_items(
                    batch_pd_files,
                    folder_root,
                    tracker,
                    concurrency=PD_PARALLEL_FOLDER_WORKERS,
                )

                # 2. Сжатие изображений в сегменте
                if task.get("compress_images"):
                    update_state(message=f"🖼️ Сжатие изображений в сегменте «{batch_name}»...")
                    compress_images_in_directory(folder_root)

                # 3. Загрузка на Google Drive
                update_task(tid, status="uploading")

                if zip_mode == "subfolders":
                    # Для плоских списков Pixeldrain «подпапок» нет — упаковываем всё в один .zip
                    zip_name = sanitize_filename(f"{folder_name} - {batch_name}") + ".zip" if len(batches) > 1 else sanitize_filename(folder_name) + ".zip"
                    tmp_zip = task_dir / zip_name
                    update_state(message=f"📦 Упаковка сегмента {zip_name}...")
                    zip_directory(folder_root, tmp_zip)
                    shutil.rmtree(folder_root, ignore_errors=True)
                    folder_root.mkdir(parents=True, exist_ok=True)
                    f_size = tmp_zip.stat().st_size
                    update_state(
                        current_file=zip_name,
                        message=f"📤 Загрузка в Drive: {zip_name} ({format_bytes(f_size)})...",
                    )
                    upload_file(tmp_zip, root_drive_id, tid, total_bytes, done_bytes_accumulated)
                    done_bytes_accumulated += batch_bytes
                    tmp_zip.unlink(missing_ok=True)

                elif zip_mode == "root":
                    zip_name = sanitize_filename(folder_name) + ".zip" if len(batches) == 1 else sanitize_filename(f"{folder_name} - {batch_name}") + ".zip"
                    upload_target_id = destination_id if len(batches) == 1 else root_drive_id
                    tmp_zip = task_dir / zip_name
                    update_state(message=f"📦 Упаковка архива {zip_name}...")
                    zip_directory(folder_root, tmp_zip)
                    shutil.rmtree(folder_root, ignore_errors=True)
                    folder_root.mkdir(parents=True, exist_ok=True)
                    f_size = tmp_zip.stat().st_size
                    update_state(
                        current_file=zip_name,
                        message=f"📤 Загрузка в Drive архива {zip_name} ({format_bytes(f_size)})...",
                    )
                    upload_file(tmp_zip, upload_target_id, tid, total_bytes, done_bytes_accumulated)
                    done_bytes_accumulated += batch_bytes
                    tmp_zip.unlink(missing_ok=True)

                else:
                    # zip_mode == "none": поштучная загрузка
                    for idx, pd_file in enumerate(batch_pd_files, 1):
                        if stop_event.is_set():
                            raise RuntimeError("Остановлено пользователем")

                        # Имя файла могло быть изменено при коллизии — ищем на диске
                        f_path = folder_root / sanitize_filename(pd_file.name)
                        if not f_path.exists():
                            # Попробуем вариант с file_id в имени (при коллизии)
                            stem = f_path.stem
                            suffix = f_path.suffix
                            alt = folder_root / f"{stem}_{pd_file.file_id}{suffix}"
                            if alt.exists():
                                f_path = alt
                        # Проверяем .jpg-конвертацию
                        if not f_path.exists():
                            jpg_alt = f_path.with_suffix(".jpg")
                            if jpg_alt.exists():
                                f_path = jpg_alt
                        if not f_path.exists():
                            add_log(f"⚠️ Файл не найден на диске: {pd_file.name}", "WARNING")
                            continue

                        f_size = f_path.stat().st_size
                        update_state(
                            current_file=f_path.name,
                            message=f"📤 Загрузка в Drive ({idx}/{len(batch_pd_files)}): {f_path.name}...",
                        )
                        upload_file(f_path, root_drive_id, tid, total_bytes, done_bytes_accumulated)
                        done_bytes_accumulated += f_size

                # 4. Сохраняем прогресс сегмента
                completed_batches.add(batch_name)
                pct = (done_bytes_accumulated / total_bytes * 100) if total_bytes > 0 else 100.0
                update_task(
                    tid,
                    completed_batches=sorted(list(completed_batches)),
                    bytes_done=done_bytes_accumulated,
                    progress=min(100.0, round(pct, 1)),
                )

                # 5. Мгновенная очистка диска Colab после выгрузки сегмента
                for pd_file in batch_pd_files:
                    (folder_root / sanitize_filename(pd_file.name)).unlink(missing_ok=True)
                    (folder_root / sanitize_filename(pd_file.name)).with_suffix(".jpg").unlink(missing_ok=True)

                add_log(f"✅ Сегмент «{batch_name}» выгружен на Google Drive (диск Colab очищен)", "OK")

            # Завершение
            update_task(tid, status="done", progress=100, bytes_done=total_bytes)
            update_state(message="Импорт завершён", overall_progress=100, current_file=None)
            add_log("Импорт списка Pixeldrain завершён успешно", "SUCCESS")
            shutil.rmtree(task_dir, ignore_errors=True)

        else:
            raise ValueError(f"Неизвестный тип Pixeldrain URL: {parsed['type']}")

    except Exception as exc:
        err = str(exc)
        tid = task["id"]
        retries = int(task.get("retries", 0)) + 1
        next_status = (
            "retry" if retries < MAX_RETRIES and not stop_event.is_set()
            else "error"
        )
        update_task(tid, status=next_status, retries=retries, error=err)
        update_state(error=err, message="Ошибка импорта Pixeldrain")
        add_log(f"Критическая ошибка Pixeldrain: {err}", "ERROR")
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
            try:
                process_task(task)
            except Exception as task_exc:
                add_log(f"Ошибка выполнения задачи {task.get('id')}: {task_exc}", "ERROR")
                add_log(traceback.format_exc(), "TRACE")
    except Exception as exc:
        add_log(f"Критический сбой цикла воркера: {exc}", "ERROR")
        add_log(traceback.format_exc(), "TRACE")
    finally:
        update_state(running=False, current_task=None, current_file=None)
        add_log("Очередь остановлена")
