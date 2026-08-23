"""
drive.py — Вся работа с Google Drive API.

Авторизация, управление квотой, создание папок,
поиск дубликатов и resumable-upload файлов.
"""
import json
import threading
import time

from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pathlib import Path

from .config import UPLOAD_CHUNK
from .helpers import add_log, update_state
from .state import stop_event

# ── Thread-local хранилище для Drive-клиента ─────────────────────────────────
_thread_local = threading.local()


def get_drive():
    """Вернуть Drive-клиент для текущего потока (создать при необходимости)."""
    if not hasattr(_thread_local, "drive"):
        creds, _ = default(scopes=["https://www.googleapis.com/auth/drive"])
        _thread_local.drive = build(
            "drive", "v3", credentials=creds, cache_discovery=False
        )
    return _thread_local.drive


# ── Квота ─────────────────────────────────────────────────────────────────────
_QUOTA_CACHE: dict = {"data": None, "last_update": 0}


def drive_about() -> dict:
    """
    Получить информацию о квоте и пользователе Google Drive.
    Результат кэшируется на 30 секунд.
    """
    now = time.time()
    if now - _QUOTA_CACHE["last_update"] > 30 or _QUOTA_CACHE["data"] is None:
        data  = get_drive().about().get(
            fields="user(displayName,emailAddress),storageQuota"
        ).execute()
        quota = data.get("storageQuota", {})
        limit       = int(quota.get("limit",         0) or 0)
        usage       = int(quota.get("usage",         0) or 0)
        usage_drive = int(quota.get("usageInDrive",  0) or 0)
        free        = max(0, limit - usage) if limit else None
        _QUOTA_CACHE["data"] = {
            "user":        data.get("user", {}),
            "limit":       limit,
            "usage":       usage,
            "usage_drive": usage_drive,
            "free":        free,
        }
        _QUOTA_CACHE["last_update"] = now
    return _QUOTA_CACHE["data"]


# ── Папки ─────────────────────────────────────────────────────────────────────

def validate_drive_folder(folder_id: str) -> bool:
    """Проверить, что folder_id указывает на папку (или root)."""
    if folder_id == "root":
        return True
    f = get_drive().files().get(fileId=folder_id, fields="id,name,mimeType").execute()
    return f["mimeType"] == "application/vnd.google-apps.folder"


def ensure_drive_folder(name: str, parent_id: str) -> str:
    """
    Найти или создать папку с именем name внутри parent_id.
    Возвращает ID папки.
    """
    name = name.strip() or "MEGA Import"
    q = (
        f"name={json.dumps(name)} and "
        f"'{parent_id}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    found = get_drive().files().list(
        q=q, fields="files(id,name)", pageSize=10
    ).execute().get("files", [])
    if found:
        return found[0]["id"]

    metadata = {
        "name":     name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents":  [parent_id],
    }
    return get_drive().files().create(body=metadata, fields="id").execute()["id"]


# ── Файлы ─────────────────────────────────────────────────────────────────────

def find_drive_file(name: str, parent_id: str, size: int) -> dict | None:
    """
    Найти файл с совпадающим именем и размером в папке parent_id.
    Возвращает метаданные файла или None.
    """
    q = (
        f"name={json.dumps(name)} and "
        f"'{parent_id}' in parents and trashed=false"
    )
    files = get_drive().files().list(
        q=q, fields="files(id,name,size,mimeType)", pageSize=50
    ).execute().get("files", [])
    for f in files:
        if f.get("size") is not None and int(f["size"]) == int(size):
            return f
    return None


def upload_file(
    file_path: Path,
    parent_id: str,
    tid: str,
    total_bytes: int,
    base_done_bytes: int,
) -> str:
    """
    Загрузить файл на Google Drive с помощью resumable upload.

    - Пропускает файл, если уже существует файл с тем же именем и размером.
    - Повторяет попытку при сетевых ошибках (до 5 раз с экспоненциальной паузой).
    - Прерывается, если установлен stop_event.

    Возвращает ID загруженного файла.
    """
    from .state import update_task  # локальный импорт во избежание цикла

    file_path = Path(file_path)
    size = file_path.stat().st_size

    existing = find_drive_file(file_path.name, parent_id, size)
    if existing:
        add_log(f"Пропуск существующего: {file_path.name}", "SKIP")
        return existing["id"]

    metadata = {"name": file_path.name, "parents": [parent_id]}
    media = MediaFileUpload(
        str(file_path),
        mimetype="application/octet-stream",
        resumable=True,
        chunksize=UPLOAD_CHUNK,
    )
    req = get_drive().files().create(
        body=metadata, media_body=media, fields="id,name,size"
    )

    chunk_retries = 0
    while True:
        if stop_event.is_set():
            raise RuntimeError("Остановлено пользователем")

        try:
            status, response = req.next_chunk()
            chunk_retries = 0

            # Пока идёт передача крупного чанка — показываем живой индикатор
            update_task(tid, progress=None)
            update_state(overall_progress=None)

            if response is not None:
                return response["id"]

        except Exception as e:
            chunk_retries += 1
            if chunk_retries > 5:
                raise e
            add_log(
                f"Сетевая ошибка Drive (попытка {chunk_retries}/5), ожидание...",
                "WARNING",
            )
            time.sleep(2 ** chunk_retries)
