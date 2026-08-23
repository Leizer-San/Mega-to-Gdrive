"""
server.py — Flask-приложение и все API-маршруты.

Читает UI из ui/index.html рядом с корнем репозитория,
чтобы HTML не был захардкожен в Python-строку.
"""
import json
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_file

from .drive import drive_about, validate_drive_folder, get_drive
from .helpers import add_log
from .mega import validate_mega_url
from .state import (
    STATE, clear_finished_tasks, create_task, get_tasks,
    load_tasks_from_disk, lock, stop_event,
)
from .worker import worker

# Путь к HTML-файлу веб-интерфейса (лежит в ui/ рядом с пакетом mega_importer/)
_UI_FILE = Path(__file__).parent.parent / "ui" / "index.html"

app = Flask(__name__)

# ── Глобальный worker-поток ───────────────────────────────────────────────────
_worker_thread: threading.Thread | None = None


# ── Страницы ──────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    """Отдаём веб-интерфейс из отдельного HTML-файла."""
    return send_file(_UI_FILE)


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/folders")
def api_folders():
    """Список подпапок Google Drive внутри parent."""
    parent = request.args.get("parent", "root").replace("'", "\\'")
    q = (
        f"'{parent}' in parents and "
        "mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    try:
        results = get_drive().files().list(
            q=q, fields="files(id, name)", pageSize=1000, orderBy="name"
        ).execute()
        return jsonify({"folders": results.get("files", [])})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.get("/api/status")
def api_status():
    """Полный снимок состояния: STATE + список задач + квота Drive."""
    try:
        quota = drive_about()
    except Exception as e:
        quota = {"error": str(e)}
    with lock:
        state = dict(STATE)
        tasks = get_tasks()
    return jsonify({"state": state, "tasks": tasks, "quota": quota})


@app.post("/api/tasks")
def api_tasks():
    """Добавить одну или несколько задач в очередь."""
    data       = request.get_json(force=True) or {}
    tasks_data = data.get("tasks", [])
    destination = str(data.get("destination") or "root").strip()

    if not tasks_data:
        return jsonify({"message": "Нет задач"}), 400

    try:
        if not validate_drive_folder(destination):
            raise ValueError("Указанный ID не является папкой.")
        added = 0
        for t in tasks_data:
            url      = str(t.get("url",      "")).strip()
            zip_mode = str(t.get("zip_mode", "none")).strip()
            if validate_mega_url(url):
                create_task(url, destination, zip_mode)
                added += 1
        return jsonify({"message": f"Добавлено задач: {added}"})
    except Exception as e:
        return jsonify({"message": str(e)}), 400


@app.post("/api/start")
def api_start():
    """Запустить обработку очереди (если не запущена)."""
    global _worker_thread
    with lock:
        if _worker_thread and _worker_thread.is_alive():
            return jsonify({"message": "Очередь уже запущена."})
        stop_event.clear()
        _worker_thread = threading.Thread(target=worker, daemon=True)
        _worker_thread.start()
    return jsonify({"message": "Очередь запущена."})


@app.post("/api/stop")
def api_stop():
    """Остановить очередь после текущей задачи."""
    stop_event.set()
    return jsonify({"message": "Остановка..."})


@app.post("/api/clear_done")
def api_clear_done():
    """Удалить завершённые задачи из очереди."""
    clear_finished_tasks()
    return jsonify({"message": "Удалено."})


# ── Точка входа ───────────────────────────────────────────────────────────────

def run(port: int | None = None) -> None:
    """
    Инициализировать состояние, запустить Flask-сервер и туннели.
    Вызывается из Colab-ноутбука.
    """
    from .config import PORT as DEFAULT_PORT
    from .config import DOWNLOAD_DIR, WORK_DIR
    from .tunnels import setup_tunnels

    _port = port or DEFAULT_PORT

    # Создаём рабочие директории
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    # Авторизация Google
    from google.colab import auth, drive as colab_drive
    print("Монтируем Google Диск для сохранения состояния...")
    colab_drive.mount("/content/drive")
    auth.authenticate_user()

    # Восстанавливаем очередь
    load_tasks_from_disk()

    # Запускаем Flask в фоне
    add_log("Запуск Flask-сервера...")
    threading.Thread(
        target=lambda: app.run(
            host="0.0.0.0", port=_port, debug=False, use_reloader=False
        ),
        daemon=True,
    ).start()

    # Настраиваем туннели и выводим ссылки
    setup_tunnels(_port)
