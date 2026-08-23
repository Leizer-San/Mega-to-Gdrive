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

# Реальный формат megadl:
# "filename.ext: 91.19% - 2.6 MiB (2,744,088 bytes) of 2.9 MiB (2.6 MiB/s)"
_PROGRESS_RE = re.compile(
    r"^(.+?):\s*"                          # имя файла
    r"([\d.]+)%"                            # процент
    r"\s*[-\u2013]+\s*"                     # тире (обычное или em-dash)
    r"([\d.,]+\s*[KMGT]?i?B)"             # скачано (читаемый размер)
    r"\s*\([\d,]+\s*bytes\)"               # скачано в байтах — игнорируем
    r"\s+of\s+"                             # of
    r"([\d.,]+\s*[KMGT]?i?B)"             # всего (читаемый размер)
    r"(?:\s+\(([\d.,]+\s*[KMGT]?i?B/s)\))?",  # скорость (опционально)
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

        # Таймаут: нет вывода > 90 секунд
        if time.time() - last_output_time > 90:
            process.kill()
            process.wait()
            raise RuntimeError(
                "⚠️ Нет ответа от MEGA более 90 секунд — "
                "возможно исчерпана квота или обрыв соединения"
            )

        line = process.stdout.readline()

        if line:
            last_output_time = time.time()
            line = line.rstrip()

            # Проверяем квоту прямо в stdout (на всякий случай)
            if _is_quota_error(line):
                process.kill()
                process.wait()
                raise RuntimeError(
                    "⚠️ Исчерпана квота MEGA (требуется смена IP/сессии)"
                )

            # Парсим прогресс и обновляем UI раз в секунду
            now = time.time()
            if now - last_update_time > 1.0:
                msg = _parse_progress(line)
                if msg:
                    update_state(message=msg)
                elif line.strip():
                    clean = re.sub(r"[\x00-\x1F\x7F-\x9F]", "", line)
                    update_state(message=clean[-120:])
                    add_log(clean, "MEGA")
                last_update_time = now

        elif retcode is not None:
            # Процесс завершился и stdout исчерпан
            break
        else:
            time.sleep(0.05)

    # Собираем stderr после завершения процесса
    try:
        stderr_output = process.stderr.read()
    except Exception:
        stderr_output = ""

    rc = process.wait()

    if stderr_output:
        for sline in stderr_output.splitlines():
            stderr_lines.append(sline)
            if sline.strip():
                add_log(sline.strip(), "MEGA-ERR")
        if _is_quota_error(stderr_output):
            raise RuntimeError(
                "⚠️ Исчерпана квота MEGA (Transfer limit exceeded)"
            )

    if rc != 0:
        err_text = "\n".join(stderr_lines) or f"megadl завершился с кодом {rc}"
        raise RuntimeError(f"megadl завершился с ошибкой (код {rc}):\n{err_text}")


def _is_quota_error(text: str) -> bool:
    """Вернуть True, если текст содержит признаки ошибки квоты."""
    t = text.lower()
    return any(kw in t for kw in _QUOTA_KEYWORDS)


def _parse_progress(line: str) -> str | None:
    """
    Разобрать строку прогресса megadl.

    Реальный формат:
      'filename.png: 91.19% – 2.6 MiB (2,744,088 bytes) of 2.9 MiB (2.6 MiB/s)'

    Возвращает красивое сообщение для UI или None если строка не прогрессная.
    """
    # Убираем ANSI-escape последовательности
    clean = re.sub(r"\x1b\[[0-9;]*m", "", line)
    m = _PROGRESS_RE.match(clean.strip())
    if not m:
        return None

    filename, pct, done, total, speed = m.groups()

    # Обрезаем длинное имя файла
    name = filename.strip()
    if len(name) > 30:
        name = "…" + name[-28:]

    parts = [f"📥 {name}  {pct}%  {done} / {total}"]
    if speed:
        parts.append(f"@ {speed}")
    return "  ".join(parts)


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
