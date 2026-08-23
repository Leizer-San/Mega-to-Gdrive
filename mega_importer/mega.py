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

# Расширенный список ключевых слов ошибки квоты MEGAcmd.
# MEGAcmd использует разные варианты в зависимости от версии и платформы.
_QUOTA_WORDS = {
    "bandwidth",
    "quota",
    "exceeded",
    "overquota",
    "eoverquota",
    "over quota",
    "rate limit",
    "rate-limit",
    "509",
    "too many",
    "transfer limit",
}


def _is_quota_line(text: str) -> bool:
    """Вернуть True, если строка содержит признак исчерпания квоты MEGA."""
    t = text.lower()
    return any(kw in t for kw in _QUOTA_WORDS)


def mega_get(url: str, target_dir: Path) -> None:
    """
    Скачать файл/папку по MEGA-ссылке в target_dir с помощью mega-get.

    Улучшения по сравнению с исходной версией:
    - stderr читается отдельным потоком — сообщения об ошибках не теряются
    - Расширенный список ключевых слов квоты (overquota, eoverquota, 509, ...)
    - Таймаут 90 сек сбрасывается при любом непустом выводе, а не только прогрессе
    - После завершения: проверка что файлы реально скачались (rc=0, но 0 файлов)
    - Точные сообщения об ошибке в логах
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["mega-get", url, str(target_dir)]
    add_log(f"MEGA: начинаю скачивание {url}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,   # ← читаем stderr отдельно
        text=True,
        errors="replace",
        bufsize=1,
    )

    # ── Читаем stderr в фоновом потоке ──────────────────────────────────────
    import threading
    stderr_lines: list[str] = []
    stderr_quota_found = threading.Event()

    def _read_stderr():
        for line in process.stderr:
            line = line.rstrip()
            if not line:
                continue
            clean = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", line)

            # Квота → немедленно сигналим основному потоку
            if _is_quota_line(clean):
                stderr_lines.append(clean)
                add_log(clean, "MEGA-ERR")
                stderr_quota_found.set()
                continue

            line_upper = clean.upper()

            # Прогресс-строки (TRANSFERRING, %, B/S) → обновляем статус, не логируем
            is_progress = any(
                x in line_upper
                for x in ["TRANSFERRING", "PROCEEDING", "%", "B/S", "DOWNLOADED"]
            )
            if is_progress:
                # Парсим прогресс: ищем "(676/1073 MB:  63.02 %)" или просто "%"
                match = re.search(r"\(([^)]+)\)", clean)
                if match and ("%" in match.group(1) or "B" in match.group(1)):
                    update_state(message=f"MEGA: {match.group(1).strip()}")
                else:
                    update_state(message="Скачивание из MEGA...")
            else:
                # Остальное — в лог (ошибки, статус, предупреждения)
                stderr_lines.append(clean)
                add_log(clean, "MEGA-ERR")

    stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
    stderr_thread.start()

    # ── Основной цикл чтения stdout ──────────────────────────────────────────
    stdout_lines: list[str] = []
    last_update_time    = time.time()
    last_activity_time  = time.time()   # сбрасывается при любом выводе
    last_seen_progress  = ""

    while True:
        # Проверяем stop_event
        if stop_event.is_set():
            process.kill()
            process.wait()
            raise RuntimeError("Остановлено пользователем")

        # Квота обнаружена в stderr — немедленно прерываем
        if stderr_quota_found.is_set():
            process.kill()
            process.wait()
            raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

        retcode = process.poll()
        if retcode is not None:
            break

        # Таймаут: нет вывода > 90 секунд
        if time.time() - last_activity_time > 90:
            process.kill()
            process.wait()
            raise RuntimeError(
                "⚠️ Нет активности от mega-get более 90 сек — "
                "вероятно исчерпана квота или обрыв соединения"
            )

        line = process.stdout.readline()
        if not line:
            time.sleep(0.05)
            continue

        line = line.strip()
        if not line:
            continue

        last_activity_time = time.time()   # любой вывод сбрасывает таймер
        stdout_lines.append(line)
        if len(stdout_lines) > 100:
            stdout_lines.pop(0)

        line_upper = line.upper()

        # Квота в stdout (MEGAcmd иногда пишет сюда)
        if _is_quota_line(line):
            process.kill()
            process.wait()
            raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

        # Прогресс-строки
        is_progress = any(
            x in line_upper
            for x in ["TRANSFERRING", "PROCEEDING", "%", "B/S", "DOWNLOADED"]
        )

        if is_progress:
            now = time.time()
            if line != last_seen_progress:
                last_seen_progress = line

            if now - last_update_time > 1.0:
                match = re.search(r"\(([^)]+)\)", line)
                if match and ("%" in match.group(1) or "B" in match.group(1)):
                    clean_msg = f"MEGA: {match.group(1).strip()}"
                else:
                    clean_msg = "Скачивание из MEGA..."
                update_state(message=clean_msg)
                last_update_time = now
        else:
            clean_line = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", line)
            update_state(message=clean_line[-100:])
            add_log(clean_line, "MEGA")

    rc = process.wait()
    stderr_thread.join(timeout=3)

    # ── Анализ результата ─────────────────────────────────────────────────────

    # 1. Квота в stderr (мог появиться после завершения процесса)
    if stderr_quota_found.is_set() or _is_quota_line("\n".join(stderr_lines)):
        raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

    # 2. Квота в накопленном stdout
    full_stdout = "\n".join(stdout_lines)
    if _is_quota_line(full_stdout):
        raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

    # 3. Ненулевой exit code
    if rc != 0:
        # Собираем контекст ошибки из обоих потоков
        err_context = "\n".join(
            stderr_lines[-10:] or stdout_lines[-10:]
        ) or f"mega-get завершился с кодом {rc}"
        raise RuntimeError(f"mega-get завершился с ошибкой (код {rc}):\n{err_context}")

    # 4. rc == 0, но файлов нет — тихий провал (бывает при квоте)
    files_downloaded = list(target_dir.rglob("*"))
    actual_files = [f for f in files_downloaded if f.is_file()]
    if not actual_files:
        hint = " (возможно исчерпана квота)" if any(
            "quota" in l.lower() or "bandwidth" in l.lower()
            for l in stdout_lines + stderr_lines
        ) else ""
        raise RuntimeError(
            f"mega-get завершился успешно, но файлы не скачались{hint}. "
            "Проверьте ссылку или квоту MEGA."
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
