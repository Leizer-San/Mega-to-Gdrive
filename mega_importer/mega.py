"""
mega.py — Всё, что связано с MEGA: скачивание, парсинг прогресса,
          работа с локальной файловой системой и упаковка в ZIP.
"""
import os
import re
import shutil
import subprocess
import time
import uuid
import zipfile
from pathlib import Path

from .helpers import add_log, update_state
from .state import stop_event

# ── Валидация URL ─────────────────────────────────────────────────────────────
MEGA_URL_RE = re.compile(r"^https?://(?:www\.)?mega\.(?:nz|co\.nz)/", re.I)


def validate_mega_url(url: str) -> bool:
    """Вернуть True, если строка выглядит как публичная MEGA-ссылка."""
    return bool(MEGA_URL_RE.match(url.strip()))


# ── Скачивание ────────────────────────────────────────────────────────────────

# Точные фразы ошибки квоты MEGAcmd (не одиночные слова во избежание ложных срабатываний)
_QUOTA_PATTERNS = [
    re.compile(r"bandwidth\s+limit\s*(reached|exceeded)", re.I),
    re.compile(r"reached\s+(your\s+)?bandwidth\s+quota", re.I),
    re.compile(r"(transfer|download)\s+quota\s*(reached|exceeded)", re.I),
    re.compile(r"quota\s+exceeded", re.I),
    re.compile(r"transfer\s+limit\s+exceeded", re.I),
    re.compile(r"download\s+limit\s*(reached|exceeded)", re.I),
    re.compile(r"try\s+again\s+in\s+\d+\s*(minutes?|hours?)", re.I),
    re.compile(r"\b(eoverquota|overquota)\b", re.I),
    re.compile(r"over\s+quota", re.I),
    re.compile(r"error:\s*509\b", re.I),
]


def _is_quota_line(text: str) -> bool:
    """Вернуть True, если строка содержит точный признак исчерпания квоты MEGA."""
    return any(p.search(text) for p in _QUOTA_PATTERNS)


def _get_quota_wait_hint(text: str) -> str:
    """Извлечь подсказку времени ожидания сброса квоты (если есть в выводе MEGA)."""
    m = re.search(r"try\s+again\s+in\s+([\d]+\s*(?:minutes?|hours?|минут|часов)?)", text, re.I)
    if m:
        return f" (сброс квоты через ~{m.group(1).strip()})"
    return ""



def mega_get(url: str, target_dir: Path) -> None:
    """
    Скачать файл/папку по MEGA-ссылке в target_dir с помощью mega-get.

    Особенности:
    - Потоки stdout и stderr вычитываются параллельно в неблокирующую очередь
    - Таймер зависания (45 сек) отслеживает сдвиг прогресса: если MEGA исчерпала квоту
      и перестала передавать данные, процесс немедленно прерывается с ясным сообщением
    - Прогресс в реальном времени транслируется в статус веб-интерфейса
    """
    import queue
    import threading

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["mega-get", "--ignore-quota-warn", url, str(target_dir)]
    add_log(f"MEGA: начинаю скачивание {url}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        errors="replace",
        bufsize=1,
    )

    output_queue: queue.Queue = queue.Queue()
    stderr_lines: list[str] = []
    stdout_lines: list[str] = []

    def _reader(pipe, stream_name: str):
        try:
            for line in iter(pipe.readline, ""):
                output_queue.put((stream_name, line))
        except Exception:
            pass
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    t_out = threading.Thread(target=_reader, args=(process.stdout, "stdout"), daemon=True)
    t_err = threading.Thread(target=_reader, args=(process.stderr, "stderr"), daemon=True)
    t_out.start()
    t_err.start()

    last_activity_time = time.time()
    last_progress_change_time = time.time()
    last_seen_progress = ""
    last_ui_update_time = 0.0

    FREEZE_TIMEOUT = 45  # 45 секунд без сдвига прогресса = исчерпание квоты / обрыв

    while True:
        if stop_event.is_set():
            process.kill()
            process.wait()
            raise RuntimeError("Остановлено пользователем")

        # Неблокирующее получение строки из очереди
        try:
            stream_name, raw_line = output_queue.get(timeout=0.5)
            has_line = True
        except queue.Empty:
            has_line = False

        now = time.time()

        if has_line and raw_line:
            clean = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", raw_line.rstrip())
            if clean:
                last_activity_time = now

                # 1. Проверяем явное сообщение о квоте
                if _is_quota_line(clean):
                    process.kill()
                    process.wait()
                    raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

                line_upper = clean.upper()
                is_progress = any(
                    x in line_upper
                    for x in ["TRANSFERRING", "PROCEEDING", "%", "B/S", "DOWNLOADED"]
                )

                if is_progress:
                    match = re.search(r"\(([^)]+)\)", clean)
                    if match and ("%" in match.group(1) or "B" in match.group(1)):
                        prog_text = match.group(1).strip()
                        # Если изменились байты / проценты — сбрасываем таймер зависания
                        if prog_text != last_seen_progress:
                            last_seen_progress = prog_text
                            last_progress_change_time = now
                        msg = f"MEGA: {prog_text}"
                    else:
                        msg = "Скачивание из MEGA..."

                    if now - last_ui_update_time > 0.8:
                        update_state(message=msg)
                        last_ui_update_time = now
                else:
                    if stream_name == "stderr":
                        stderr_lines.append(clean)
                        add_log(clean, "MEGA-ERR")
                    else:
                        stdout_lines.append(clean)
                        add_log(clean, "MEGA")

        # Проверяем, завершился ли процесс
        retcode = process.poll()
        if retcode is not None and output_queue.empty():
            break

        # 2. Детекция скрытого исчерпания квоты / зависания:
        # Если нет данных вообще или прогресс остановился на одном месте > 45 сек
        time_since_progress = now - last_progress_change_time
        time_since_activity = now - last_activity_time

        if time_since_progress > FREEZE_TIMEOUT and time_since_activity > 10:
            process.kill()
            process.wait()
            prog_info = f" на отметке [{last_seen_progress}]" if last_seen_progress else ""
            raise RuntimeError(
                f"⚠️ Исчерпана квота MEGA или зависло соединение{prog_info}: "
                f"нет прогресса более {FREEZE_TIMEOUT} сек. Требуется смена IP/сессии."
            )

    rc = process.wait()
    t_out.join(timeout=2)
    t_err.join(timeout=2)

    # ── Анализ результата ─────────────────────────────────────────────────────

    files_downloaded = list(target_dir.rglob("*"))
    actual_files = [f for f in files_downloaded if f.is_file()]

    # Успех
    if rc == 0 and actual_files:
        return

    # Ошибки
    all_output = "\n".join(stderr_lines + stdout_lines)
    if _is_quota_line(all_output):
        wait_hint = _get_quota_wait_hint(all_output)
        raise RuntimeError(f"⚠️ Исчерпана квота MEGA{wait_hint} (требуется смена IP/прокси)")


    if rc != 0:
        err_context = "\n".join(
            stderr_lines[-10:] or stdout_lines[-10:]
        ) or f"mega-get завершился с кодом {rc}"
        raise RuntimeError(f"mega-get завершился с ошибкой (код {rc}):\n{err_context}")

    if not actual_files:
        raise RuntimeError(
            "mega-get завершился без ошибок, но файлы не были сохранены. "
            "Проверьте доступность ссылки или квоту MEGA."
        )




# ── Утилиты для работы с локальными файлами ──────────────────────────────────

def all_files(root: Path):
    """Рекурсивно перебрать все файлы в директории."""
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def local_tree_stats(root: Path) -> tuple[int, int]:
    """Вернуть (количество файлов, суммарный размер в байтах)."""
    count = 0
    total = 0
    for f in all_files(root):
        try:
            total += f.stat().st_size
            count += 1
        except OSError:
            pass
    return count, total


def build_drive_tree(local_root: Path, root_drive_id: str) -> dict:
    """
    Создать зеркальную структуру папок в Google Drive.

    Возвращает словарь {relative_path → drive_folder_id}.
    """
    from .drive import ensure_drive_folder  # избегаем циклического импорта

    mapping = {Path("."): root_drive_id}
    dirs = sorted(
        [p for p in local_root.rglob("*") if p.is_dir()],
        key=lambda x: len(x.relative_to(local_root).parts),
    )
    for d in dirs:
        rel = d.relative_to(local_root)
        parent_id  = mapping[rel.parent]
        mapping[rel] = ensure_drive_folder(d.name, parent_id)
    return mapping


# ── ZIP-упаковка ──────────────────────────────────────────────────────────────

def zip_directory(dir_path: Path, zip_path: Path) -> None:
    """Упаковать содержимое dir_path в zip_path без сжатия (ZIP_STORED)."""
    with zipfile.ZipFile(
        zip_path, "w", zipfile.ZIP_STORED, allowZip64=True
    ) as zipf:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname   = os.path.relpath(file_path, dir_path)
                zipf.write(file_path, arcname)


def apply_zip_mode(task_dir: Path, zip_mode: str) -> None:
    """
    Применить zip_mode к скачанному содержимому task_dir.

    zip_mode:
        "none"       — не архивировать
        "root"       — весь контент в один .zip
        "subfolders" — каждую подпапку в отдельный .zip
    """
    from .helpers import sanitize_filename

    entries = list(task_dir.iterdir())
    is_single_file = len(entries) == 1 and entries[0].is_file()

    if zip_mode == "none" or is_single_file:
        return

    update_state(message="Архивация файлов без сжатия...")

    if zip_mode == "root":
        if len(entries) == 1 and entries[0].is_dir():
            zip_name   = sanitize_filename(entries[0].name) + ".zip"
            source_dir = entries[0]
        else:
            zip_name   = "MEGA_Archive.zip"
            source_dir = task_dir

        tmp_zip = task_dir.parent / (task_dir.name + "_tmp.zip")
        zip_directory(source_dir, tmp_zip)
        shutil.rmtree(task_dir)
        task_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(tmp_zip), str(task_dir / zip_name))

    elif zip_mode == "subfolders":
        actual_root = entries[0] if (len(entries) == 1 and entries[0].is_dir()) else task_dir
        for item in list(actual_root.iterdir()):
            if item.is_dir():
                update_state(message=f"Упаковка папки {item.name}...")
                tmp_zip  = task_dir.parent / (uuid.uuid4().hex + ".zip")
                zip_directory(item, tmp_zip)
                shutil.rmtree(item)
                zip_name = sanitize_filename(item.name) + ".zip"
                shutil.move(str(tmp_zip), str(actual_root / zip_name))
