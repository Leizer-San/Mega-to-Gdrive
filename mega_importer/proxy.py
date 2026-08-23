"""
proxy.py — Менеджер пула прокси для MEGA.

Возможности:
  - Парсинг различных форматов (ip:port, ip:port:user:pass, protocol://user:pass@ip:port)
  - Полная поддержка прокси с логином и паролем (URL-encoding)
  - Автоопределение протокола (HTTP, SOCKS5, SOCKS4) и проверка доступности с замером пинга
  - Управление настройками mega-proxy (применение, сброс, ротация)
  - Контроль работы демона mega-cmd-server (автоматический перезапуск при сбоях)
  - Автоматическое сохранение/загрузка списка с Google Drive
"""
from __future__ import annotations

import json
import re
import socket
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import PROXIES_FILE
from .helpers import add_log


# ── Управление демоном MEGAcmd ────────────────────────────────────────────────

def ensure_megacmd_server_running() -> bool:
    """Убедиться, что mega-cmd-server работает и отвечает."""
    try:
        res = subprocess.run(
            ["mega-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=8,
        )
        if res.returncode == 0 and "stopped" not in res.stdout.lower():
            return True
    except Exception:
        pass

    # Если сервер не отвечает — запускаем его
    try:
        subprocess.Popen(
            ["mega-cmd-server"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        time.sleep(2)
    except Exception:
        pass
    return True


# ── Парсинг строк прокси ──────────────────────────────────────────────────────

def parse_proxy_string(text: str) -> dict | None:
    """
    Разобрать строку прокси в словарь параметров.

    Поддерживаемые форматы:
      - http://user:pass@1.2.3.4:8080
      - socks5://1.2.3.4:1080
      - 1.2.3.4:8080:user:pass
      - user:pass@1.2.3.4:8080
      - 1.2.3.4:8080
    """
    text = text.strip()
    if not text or text.startswith("#"):
        return None

    protocol = "unknown"
    username = None
    password = None

    # 1. Извлекаем схему (если есть)
    if "://" in text:
        parts = text.split("://", 1)
        protocol = parts[0].lower().strip()
        text = parts[1]

    # 2. Формат user:pass@host:port
    if "@" in text:
        auth_part, host_part = text.split("@", 1)
        if ":" in auth_part:
            username, password = auth_part.split(":", 1)
        else:
            username = auth_part
        text = host_part

    # 3. Формат host:port:user:pass или host:port
    pieces = text.split(":")
    if len(pieces) >= 4 and username is None:
        host = pieces[0].strip()
        port = int(pieces[1].strip())
        username = pieces[2].strip()
        password = ":".join(pieces[3:]).strip()
    elif len(pieces) >= 2:
        host = pieces[0].strip()
        try:
            port = int(pieces[1].strip())
        except ValueError:
            return None
    else:
        return None

    if not host or port <= 0 or port > 65535:
        return None

    return {
        "id": uuid.uuid4().hex[:10],
        "host": host,
        "port": port,
        "protocol": protocol,  # "http", "https", "socks5", "socks4", "unknown"
        "username": username,
        "password": password,
        "status": "untested",  # "online", "offline", "untested", "quota_exceeded"
        "ping": None,
        "error": None,
        "last_checked": None,
    }


def format_proxy_url(p: dict) -> str:
    """Вернуть полный URL прокси для mega-proxy с поддержкой логина/пароля."""
    proto = p.get("protocol", "http")
    if proto in ("unknown", "https"):
        proto = "http"
    if proto == "socks5":
        proto = "socks5h"  # DNS resolution via proxy
    elif proto == "socks4":
        proto = "socks4a"

    host = p["host"]
    port = p["port"]
    user = p.get("username")
    pwd = p.get("password")

    if user and pwd:
        safe_user = urllib.parse.quote(user, safe="")
        safe_pwd = urllib.parse.quote(pwd, safe="")
        return f"{proto}://{safe_user}:{safe_pwd}@{host}:{port}"
    return f"{proto}://{host}:{port}"


# ── Проверка доступности и автоопределение протокола ──────────────────────────

def _test_single_protocol(host: str, port: int, protocol: str, username: str | None, password: str | None, timeout: float = 4.0) -> tuple[bool, int, str | None]:
    """
    Протестировать конкретный протокол.
    Возвращает (success: bool, latency_ms: int, error_message: str | None).
    """
    import requests

    scheme = "http"
    if protocol == "socks5":
        scheme = "socks5h"
    elif protocol == "socks4":
        scheme = "socks4a"

    if username and password:
        safe_u = urllib.parse.quote(username, safe="")
        safe_p = urllib.parse.quote(password, safe="")
        proxy_url = f"{scheme}://{safe_u}:{safe_p}@{host}:{port}"
    else:
        proxy_url = f"{scheme}://{host}:{port}"

    proxies = {"http": proxy_url, "https": proxy_url}
    test_url = "http://connectivitycheck.gstatic.com/generate_204"

    start_time = time.time()
    try:
        resp = requests.get(test_url, proxies=proxies, timeout=timeout)
        latency = int((time.time() - start_time) * 1000)
        if resp.status_code in (200, 204):
            return True, latency, None
        return False, latency, f"HTTP {resp.status_code}"
    except Exception as e:
        err_msg = str(e)
        if "timeout" in err_msg.lower():
            err_msg = "Таймаут"
        elif "connection refused" in err_msg.lower():
            err_msg = "Отказ в соединении"
        elif "proxy" in err_msg.lower() or "socks" in err_msg.lower():
            err_msg = "Ошибка прокси"
        else:
            err_msg = err_msg[:50]
        return False, 0, err_msg


def check_proxy(p: dict) -> dict:
    """
    Проверить работоспособность прокси и автоопределить протокол, если он не указан.
    """
    host = p["host"]
    port = p["port"]
    proto = p.get("protocol", "unknown")
    user = p.get("username")
    pwd = p.get("password")

    p["status"] = "checking"

    protocols_to_try = [proto] if proto != "unknown" else ["http", "socks5", "socks4"]

    for pr in protocols_to_try:
        ok, latency, err = _test_single_protocol(host, port, pr, user, pwd, timeout=4.0)
        if ok:
            p["protocol"] = pr
            p["status"] = "online"
            p["ping"] = latency
            p["error"] = None
            p["last_checked"] = datetime.now(timezone.utc).isoformat()
            return p

    p["status"] = "offline"
    p["ping"] = None
    p["error"] = err or "Не отвечает"
    p["last_checked"] = datetime.now(timezone.utc).isoformat()
    return p


# ── Менеджер пула прокси ──────────────────────────────────────────────────────

class ProxyManager:
    """Потокобезопасный менеджер пула прокси."""

    def __init__(self):
        self._lock = threading.RLock()
        self.proxies: list[dict] = []
        self.active_proxy_id: str | None = None
        self.auto_rotate: bool = True

    def load_from_disk(self) -> None:
        """Загрузить прокси из Google Drive."""
        with self._lock:
            if PROXIES_FILE.exists():
                try:
                    with open(PROXIES_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        self.proxies = data.get("proxies", [])
                        self.auto_rotate = data.get("auto_rotate", True)
                    print(f"✅ Загружено прокси из Google Диска: {len(self.proxies)}")
                except Exception as e:
                    print(f"⚠️ Ошибка чтения файла прокси: {e}")

    def save_to_disk(self) -> None:
        """Сохранить прокси на Google Drive."""
        with self._lock:
            try:
                PROXIES_FILE.parent.mkdir(parents=True, exist_ok=True)
                with open(PROXIES_FILE, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "proxies": self.proxies,
                            "auto_rotate": self.auto_rotate,
                            "updated_at": datetime.now(timezone.utc).isoformat(),
                        },
                        f,
                        ensure_ascii=False,
                        indent=2,
                    )
            except Exception:
                pass

    def add_proxies_text(self, text: str) -> list[dict]:
        """Разобрать многострочный текст и добавить новые прокси."""
        new_items = []
        with self._lock:
            existing_keys = {f"{p['host']}:{p['port']}" for p in self.proxies}
            for line in text.strip().splitlines():
                parsed = parse_proxy_string(line)
                if parsed:
                    key = f"{parsed['host']}:{parsed['port']}"
                    if key not in existing_keys:
                        self.proxies.append(parsed)
                        existing_keys.add(key)
                        new_items.append(parsed)
            self.save_to_disk()

        # Фоновая проверка добавленных прокси
        if new_items:
            threading.Thread(target=self.check_all, daemon=True).start()

        return new_items

    def check_all(self) -> None:
        """Проверить все прокси в пуле параллельно."""
        with self._lock:
            items = list(self.proxies)

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(check_proxy, items))

        with self._lock:
            for res in results:
                for idx, p in enumerate(self.proxies):
                    if p["id"] == res["id"]:
                        self.proxies[idx] = res
                        break
            self.save_to_disk()

    def remove_proxy(self, proxy_id: str) -> None:
        """Удалить прокси по ID."""
        with self._lock:
            if self.active_proxy_id == proxy_id:
                self.disable_megacmd_proxy()
            self.proxies = [p for p in self.proxies if p["id"] != proxy_id]
            self.save_to_disk()

    def clear_dead(self) -> int:
        """Удалить все неработающие прокси."""
        with self._lock:
            before = len(self.proxies)
            self.proxies = [p for p in self.proxies if p["status"] == "online"]
            self.save_to_disk()
            return before - len(self.proxies)

    def get_state(self) -> dict:
        """Получить снимок состояния для веб-интерфейса."""
        with self._lock:
            return {
                "proxies": list(self.proxies),
                "active_proxy_id": self.active_proxy_id,
                "auto_rotate": self.auto_rotate,
                "count_total": len(self.proxies),
                "count_online": sum(1 for p in self.proxies if p["status"] == "online"),
            }

    # ── Управление MEGAcmd Proxy ──────────────────────────────────────────────

    def apply_megacmd_proxy(self, proxy: dict) -> bool:
        """Применить прокси в MEGAcmd через mega-proxy с передачей флагов авторизации."""
        ensure_megacmd_server_running()

        proto = (proxy.get("protocol") or "http").lower().strip()
        if proto in ("unknown", "https"):
            proto = "http"
        elif proto == "socks5":
            proto = "socks5h"
        elif proto == "socks4":
            proto = "socks4a"

        host = proxy["host"]
        port = proxy["port"]
        user = proxy.get("username")
        pwd  = proxy.get("password")

        url = f"{proto}://{host}:{port}"
        cmd = ["mega-proxy", url]
        if user:
            cmd.append(f"--username={user}")
        if pwd:
            cmd.append(f"--password={pwd}")

        display_auth = " (с логином и паролем)" if user else ""
        add_log(f"PROXY: Применяю mega-proxy -> {proto.upper()}://{host}:{port}{display_auth}")
        
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
        if res.returncode == 0 or "PROXY_CUSTOM" in res.stdout:
            with self._lock:
                self.active_proxy_id = proxy["id"]
            add_log(f"✅ Прокси успешно подключен: {host}:{port} ({proto.upper()})")
            return True
        else:
            add_log(f"⚠️ Ошибка настройки mega-proxy: {res.stdout.strip()}", level="WARNING")
            return False


    def disable_megacmd_proxy(self) -> None:
        """Отключить прокси в MEGAcmd (прямое соединение)."""
        ensure_megacmd_server_running()
        subprocess.run(["mega-proxy", "--none"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        with self._lock:
            self.active_proxy_id = None
        add_log("PROXY: Отключен (используется прямое подключение)")

    def get_next_working_proxy(self) -> dict | None:
        """
        Выбрать следующий рабочий прокси из пула.
        Возвращает словарь прокси или None, если рабочих прокси нет.
        """
        with self._lock:
            online_proxies = [p for p in self.proxies if p["status"] == "online"]
            if not online_proxies:
                return None

            # Ищем следующий после текущего активного
            if self.active_proxy_id:
                ids = [p["id"] for p in online_proxies]
                if self.active_proxy_id in ids:
                    idx = ids.index(self.active_proxy_id)
                    next_proxy = online_proxies[(idx + 1) % len(online_proxies)]
                    return next_proxy

            return online_proxies[0]

    def rotate_on_quota(self, mark_as: str = "quota_exceeded", error_msg: str = "Квота исчерпана") -> bool:
        """
        Сменить прокси при исчерпании квоты MEGA или отказе прокси.
        Возвращает True, если удалось переключиться на следующий рабочий прокси.
        """
        with self._lock:
            if not self.auto_rotate:
                return False

            # Помечаем текущий прокси
            if self.active_proxy_id:
                for p in self.proxies:
                    if p["id"] == self.active_proxy_id:
                        p["status"] = mark_as
                        p["error"] = error_msg[:50]
                        break
                self.save_to_disk()

            next_p = self.get_next_working_proxy()
            if not next_p:
                add_log("⚠️ Нет доступных рабочих прокси для ротации! Отключаю mega-proxy (прямое соединение)...", level="WARNING")
                self.disable_megacmd_proxy()
                return False

        add_log(f"🔄 Ротация прокси -> {next_p['host']}:{next_p['port']} ({next_p.get('protocol', '').upper()})")
        return self.apply_megacmd_proxy(next_p)


# Глобальный синглтон менеджера прокси
proxy_manager = ProxyManager()
