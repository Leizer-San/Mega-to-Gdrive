"""
server.py — Flask-приложение и все API-маршруты.

Читает UI из ui/index.html рядом с корнем репозитория,
чтобы HTML не был захардкожен в Python-строку.
"""
import json
import re
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_file

from .drive import drive_about, validate_drive_folder, get_drive
from .helpers import add_log
from .mega import validate_mega_url
from .proxy import proxy_manager
from .state import (
    STATE, clear_finished_tasks, clear_all_tasks, create_task, get_tasks,
    load_tasks_from_disk, lock, restart_errored_tasks, stop_event,
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


# ── API: Задачи и Google Drive ────────────────────────────────────────────────

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
    """Полный снимок состояния: STATE + список задач + квота Drive + прокси."""
    try:
        quota = drive_about()
    except Exception as e:
        quota = {"error": str(e)}
    with lock:
        state = dict(STATE)
        tasks = get_tasks()
    proxy_state = proxy_manager.get_state()
    return jsonify({"state": state, "tasks": tasks, "quota": quota, "proxies": proxy_state})


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
            raw_url  = str(t.get("url",      "")).strip()
            zip_mode = str(t.get("zip_mode", "none")).strip()
            sub_urls = [u.strip() for u in re.split(r"[\r\n;]+", raw_url) if u.strip()]
            for url in sub_urls:
                if validate_mega_url(url):
                    create_task(url, destination, zip_mode)
                    added += 1
        return jsonify({"message": f"Добавлено задач: {added}"})
    except Exception as e:
        return jsonify({"message": str(e)}), 400


@app.post("/api/start")
def api_start():
    """Запустить обработку очереди. Автоматически переводит задачи с ошибками в queued."""
    global _worker_thread
    with lock:
        restarted = restart_errored_tasks()
        stop_event.clear()
        if not (_worker_thread and _worker_thread.is_alive()):
            _worker_thread = threading.Thread(target=worker, daemon=True)
            _worker_thread.start()
    msg = f"Очередь запущена (возобновлено задач: {restarted})" if restarted else "Очередь запущена."
    return jsonify({"message": msg})


@app.post("/api/restart_errors")
def api_restart_errors():
    """Сбросить статус всех задач с ошибкой на queued и гарантированно запустить воркер."""
    global _worker_thread
    with lock:
        restarted = restart_errored_tasks()
        stop_event.clear()
        if not (_worker_thread and _worker_thread.is_alive()):
            _worker_thread = threading.Thread(target=worker, daemon=True)
            _worker_thread.start()
    msg = f"Возобновлено задач: {restarted}. Очередь запущена." if restarted else "Очередь запущена."
    return jsonify({"message": msg})


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


@app.post("/api/clear_all")
def api_clear_all():
    """Очистить всю очередь."""
    clear_all_tasks()
    return jsonify({"message": "Очередь очищена."})


# ── API: Прокси ───────────────────────────────────────────────────────────────

@app.get("/api/proxies")
def api_proxies_get():
    """Получить список прокси и статус ротации."""
    return jsonify(proxy_manager.get_state())


@app.post("/api/proxies/add")
def api_proxies_add():
    """Добавить прокси из текстового списка."""
    data = request.get_json(force=True) or {}
    text = data.get("text", "")
    added = proxy_manager.add_proxies_text(text)
    return jsonify({"message": f"Добавлено: {len(added)}", "added_count": len(added)})


@app.post("/api/proxies/check")
def api_proxies_check():
    """Запустить параллельную проверку всех прокси."""
    threading.Thread(target=proxy_manager.check_all, daemon=True).start()
    return jsonify({"message": "Проверка запущена в фоне..."})


@app.post("/api/proxies/delete")
def api_proxies_delete():
    """Удалить конкретный прокси."""
    data = request.get_json(force=True) or {}
    pid = data.get("id")
    if pid:
        proxy_manager.remove_proxy(pid)
        return jsonify({"message": "Удалено"})
    return jsonify({"error": "ID не указан"}), 400


@app.post("/api/proxies/clear_dead")
def api_proxies_clear_dead():
    """Очистить все неработающие прокси."""
    removed = proxy_manager.clear_dead()
    return jsonify({"message": f"Удалено неработающих: {removed}"})


@app.post("/api/proxies/toggle_auto")
def api_proxies_toggle_auto():
    """Включить/выключить авторотацию прокси при квоте."""
    data = request.get_json(force=True) or {}
    enabled = bool(data.get("enabled", True))
    proxy_manager.auto_rotate = enabled
    proxy_manager.save_to_disk()
    return jsonify({"auto_rotate": proxy_manager.auto_rotate})


@app.post("/api/proxies/reset_quota")
def api_proxies_reset_quota():
    """Сбросить метки quota_exceeded обратно на online для повторной ротации."""
    count = proxy_manager.reset_quota_marks()
    return jsonify({"message": f"Сброшено квот: {count}", "reset_count": count})



@app.post("/api/proxies/use")
def api_proxies_use():
    """Вручную выбрать активный прокси или отключить (id=null)."""
    data = request.get_json(force=True) or {}
    pid = data.get("id")
    if pid is None:
        proxy_manager.disable_megacmd_proxy(restart=False)
        return jsonify({"message": "Используется прямое подключение"})

    target_proxy = next((p for p in proxy_manager.proxies if p["id"] == pid), None)
    if not target_proxy:
        return jsonify({"error": "Прокси не найден"}), 404

    ok = proxy_manager.apply_megacmd_proxy(target_proxy, restart=False)
    if ok:
        display_name = target_proxy.get("display_name") or f"{target_proxy['host']}:{target_proxy['port']}"
        return jsonify({"message": f"Подключено: {display_name}"})
    return jsonify({"error": "Не удалось применить прокси"}), 400


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

    # Восстанавливаем очередь и пул прокси
    load_tasks_from_disk()
    proxy_manager.load_from_disk()
    if proxy_manager.proxies:
        threading.Thread(target=proxy_manager.check_all, daemon=True).start()

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
