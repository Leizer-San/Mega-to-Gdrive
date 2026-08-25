"""
state.py — Глобальное состояние очереди, блокировки и персистентность.

Хранит STATE (dict), threading-примитивы и функции
для создания/обновления задач и сохранения очереди на Google Drive.
"""
import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .config import STATE_FILE, PERSISTENT_DIR

# ── Глобальные примитивы ─────────────────────────────────────────────────────
lock        = threading.RLock()
stop_event  = threading.Event()
worker_thread: threading.Thread | None = None

# ── Основной словарь состояния ───────────────────────────────────────────────
STATE: dict = {
    "running":          False,
    "current_task":     None,
    "current_file":     None,
    "overall_progress": 0.0,
    "message":          "Готово к работе",
    "error":            None,
    "logs":             [],
    "tasks":            [],
}


# ── Персистентность ───────────────────────────────────────────────────────────

def load_tasks_from_disk() -> None:
    """Восстановить очередь из JSON-файла на Google Drive при старте."""
    PERSISTENT_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                saved_tasks = json.load(f)
            # Задачи в процессе выполнения при перезапуске ставим в очередь
            for t in saved_tasks:
                if t["status"] in ("downloading", "uploading"):
                    t["status"] = "queued"
            STATE["tasks"] = saved_tasks
            print("✅ Очередь успешно восстановлена из Google Диска.")
        except Exception as e:
            print(f"⚠️ Ошибка чтения файла состояния: {e}")


def save_tasks_to_disk() -> None:
    """Сохранить текущую очередь в JSON-файл на Google Drive."""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE["tasks"], f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ── Операции над задачами ─────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_task(
    url: str,
    destination_id: str,
    zip_mode: str = "none",
    compress_images: bool = False,
) -> str:
    """Создать новую задачу и добавить её в очередь. Возвращает task ID."""
    tid = uuid.uuid4().hex
    task = {
        "id":              tid,
        "url":             url,
        "name":            url.split("/")[-1] if "/" in url else url,
        "status":          "queued",
        "progress":        0.0,
        "bytes_total":     0,
        "bytes_done":      0,
        "destination_id":  destination_id,
        "zip_mode":        zip_mode,
        "compress_images": bool(compress_images),
        "retries":         0,
        "error":           None,
        "created_at":      _now_iso(),
    }
    with lock:
        STATE["tasks"].append(task)
        save_tasks_to_disk()
    return tid


def update_task(task_id: str, **kwargs) -> None:
    """Обновить поля задачи по ID и сохранить на диск."""
    with lock:
        for t in STATE["tasks"]:
            if t["id"] == task_id:
                t.update(kwargs)
                break
        save_tasks_to_disk()


def get_tasks() -> list:
    """Вернуть копию списка всех задач."""
    with lock:
        return list(STATE["tasks"])


def get_next_task() -> dict | None:
    """Вернуть следующую задачу со статусом queued/retry, или None."""
    with lock:
        for t in STATE["tasks"]:
            if t["status"] in ("queued", "retry"):
                return dict(t)
    return None


def restart_errored_tasks() -> int:
    """Сбросить статус всех задач с ошибкой на 'queued', чтобы они повторились при запуске."""
    count = 0
    with lock:
        for t in STATE["tasks"]:
            if t["status"] in ("error", "retry"):
                t["status"] = "queued"
                t["retries"] = 0
                t["error"] = None
                count += 1
        save_tasks_to_disk()
    return count


def clear_finished_tasks() -> None:
    """Удалить из очереди все завершённые (done) задачи."""
    with lock:
        STATE["tasks"] = [t for t in STATE["tasks"] if t["status"] != "done"]
        save_tasks_to_disk()


def clear_all_tasks() -> None:
    """Очистить всю очередь задач и удалить временные папки."""
    import shutil
    with lock:
        for t in STATE["tasks"]:
            td = DOWNLOAD_DIR / t["id"]
            shutil.rmtree(td, ignore_errors=True)
        STATE["tasks"] = []
        save_tasks_to_disk()

