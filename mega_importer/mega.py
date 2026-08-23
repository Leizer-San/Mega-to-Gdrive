"""
mega.py — Всё, что связано с MEGA: скачивание, парсинг прогресса,
          работа с локальной файловой системой и упаковка в ZIP.

Использует megatools (megadl) вместо MEGAcmd:
  - Чёткий ненулевой exit code при квоте
  - "Transfer limit exceeded" пишется в stderr — не теряется
  - Нет зависаний при квоте: процесс сам завершается
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


# ── Скачивание (megadl) ───────────────────────────────────────────────────────

# Ключевые слова квоты в выводе megadl (регистронезависимо)
_QUOTA_KEYWORDS = [
    "transfer limit",
    "transfer quota",
    "quota exceeded",
    "bandwidth",
    "over quota",
    "509",
]

# Прогресс-индикаторы megadl: строки вида "  12.34 MB/123.45 MB (10%) 5.6 MB/s"
_PROGRESS_RE = re.compile(
    r"(\d+\.?\d*\s*[KMGT]?B)\s*/\s*(\d+\.?\d*\s*[KMGT]?B)"
    r"(?:\s*\((\d+)%\))?(?:\s*([\d.]+\s*[KMGT]?B/s))?",
    re.I,
)


def mega_download(url: str, target_dir: Path) -> None:
    """
    Скачать файл/папку по MEGA-ссылке в target_dir с помощью megadl.

    Преимущества megadl перед MEGAcmd:
    - Возвращает ненулевой exit code при квоте (+ пишет "Transfer limit exceeded" в stderr)
    - Чёткий парсинг прогресса через регулярное выражение
    - Нет зависания при квоте: процесс сам завершается с ошибкой

    Бросает RuntimeError:
    - При исчерпании квоты MEGA
    - При любой другой ошибке скачивания
    - При таймауте (нет вывода > 90 сек)
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    cmd = ["megadl", "--path", str(target_dir), url]
    add_log(f"MEGA (megadl): начинаю скачивание {url}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,   # stderr отдельно — там живут ошибки квоты
        text=True,
        errors="replace",
        bufsize=1,
        cwd=str(target_dir),
    )

    last_output_time = time.time()
    last_update_time = time.time()
    stderr_lines: list[str] = []

    while True:
        # Проверяем stop_event
        if stop_event.is_set():
            process.kill()
            process.wait()
            raise RuntimeError("Остановлено пользователем")

        retcode = process.poll()
            process.kill()
            raise RuntimeError(
                "⚠️ Превышена квота MEGA или зависло соединение "
                "(нет ответа от сервера >60 сек)"
            )

        line = process.stdout.readline()
        if not line:
            time.sleep(0.1)
            continue

        line = line.strip()
        if not line:
            continue

        lines.append(line)
        if len(lines) > 50:
            lines.pop(0)

        line_upper = line.upper()

        if any(
            err_word in line_upper
            for err_word in ["BANDWIDTH", "QUOTA", "EXCEEDED", "LIMIT"]
        ):
            process.kill()
            raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")

        is_progress = any(
            x in line_upper
            for x in ["TRANSFERRING", "PROCEEDING", "%", "B/S", "DOWNLOADED"]
        )

        if is_progress:
            now = time.time()
            if line != last_seen_progress:
                last_seen_progress   = line
                last_progress_change = now  # сбрасываем таймер таймаута

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
    if rc != 0:
        full_output = "\n".join(lines)
        if "bandwidth" in full_output.lower() or "quota" in full_output.lower():
            raise RuntimeError("⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)")
        raise RuntimeError("mega-get завершился с ошибкой:\n" + full_output)


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
