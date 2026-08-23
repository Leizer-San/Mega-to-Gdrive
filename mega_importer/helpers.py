"""
helpers.py — Вспомогательные функции: форматирование, логи, утилиты.
"""
import re
from datetime import datetime, timezone

from .state import STATE, lock


def now_iso() -> str:
    """Текущее время в формате ISO 8601 (UTC)."""
    return datetime.now(timezone.utc).isoformat()


def format_bytes(n) -> str:
    """Человекочитаемый размер файла (B / KB / MB / GB / TB)."""
    if n is None:
        return "—"
    n = float(n)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for unit in units:
        if n < 1024 or unit == units[-1]:
            return f"{n:.1f} {unit}"
        n /= 1024
    return "—"


def add_log(message: str, level: str = "INFO") -> None:
    """Добавить запись в лог (STATE + stdout)."""
    stamp = datetime.now().strftime("%H:%M:%S")
    item = f"[{stamp}] {level}: {message}"
    with lock:
        STATE["logs"].append(item)
        STATE["logs"] = STATE["logs"][-300:]
    print(item, flush=True)


def update_state(**kwargs) -> None:
    """Атомарно обновить поля в глобальном STATE."""
    with lock:
        STATE.update(kwargs)


def sanitize_filename(name: str) -> str:
    """Убрать символы, недопустимые в именах файлов."""
    return re.sub(r'[\\/*?:"<>|]', "_", name)
