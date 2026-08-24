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
from .state import stop_event, update_task

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



def mega_get(url: str, target_dir: Path, task_id: str | None = None) -> None:
    """
    Скачать файл/папку по MEGA-ссылке в target_dir с помощью нативного загрузчика.
    Поддерживает прямые Range-запросы, почанковую ротацию прокси и AES-128-CTR расшифровку.
    """
    from .native_downloader import download_mega_url

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    add_log(f"MEGA: начинаю скачивание {url}")
    update_state(message="MEGA: подключение к серверу и проверка файлов…")

    downloaded = download_mega_url(url, target_dir, task_id=task_id)
    if not downloaded:
        raise RuntimeError(
            "MEGA API завершил работу, но файлы не были сохранены. "
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
