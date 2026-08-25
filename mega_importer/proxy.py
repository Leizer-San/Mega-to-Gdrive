"""
proxy.py — Менеджер пула прокси для MEGA.

Возможности:
  - Парсинг форматов: ip:port, ip:port:user:pass, protocol://user:pass@ip:port
  - Поддержка нескольких учётных записей на одном host:port
  - Автоопределение протокола (HTTP, SOCKS5, SOCKS4) с замером пинга
  - Управление mega-proxy (применение, отключение, ротация)
  - Контроль демона mega-cmd-server (автоперезапуск при сбоях)
  - Счётчик ротаций и защита от бесконечных циклов
  - Персистентное хранение на Google Drive
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from .config import PROXIES_FILE
from .helpers import add_log


# ═══════════════════════════════════════════════════════════════════════════════
# Демон MEGAcmd
# ═══════════════════════════════════════════════════════════════════════════════

def ensure_megacmd_server_running() -> None:
    """Убедиться, что mega-cmd-server работает и отвечает на команды."""
    for attempt in range(3):
        try:
            res = subprocess.run(
                ["mega-version"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=8,
            )
            if res.returncode == 0 and "stopped" not in res.stdout.lower():
                return
        except Exception:
            pass

        # Запускаем сервер
        try:
            subprocess.Popen(
                ["mega-cmd-server"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except Exception:
            pass
        time.sleep(2 + attempt)


def restart_megacmd_server() -> None:
    """
    Полный перезапуск mega-cmd-server с очисткой кеша сессии.

    MEGAcmd кеширует состояние квоты и привязку IP внутри демона
    и в файле ~/.megaCmd/session. Простая смена прокси через mega-proxy
    НЕ сбрасывает этот кеш — демон продолжает считать, что квота
    исчерпана на старом IP. Поэтому при ротации прокси необходимо:
      1. Остановить демон (mega-quit)
      2. Убить оставшиеся процессы (killall)
      3. Удалить файл сессии (~/.megaCmd/session)
      4. Запустить демон заново
      5. Дождаться готовности (mega-version)
    """
    from pathlib import Path

    add_log("PROXY: Перезапуск mega-cmd-server (очистка кеша сессии)...")

    # 1. Мягкая остановка
    try:
        subprocess.run(
            ["mega-quit"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=8,
        )
    except Exception:
        pass
    time.sleep(1)

    # 2. Жёсткое завершение оставшихся процессов
    try:
        subprocess.run(
            ["killall", "-9", "mega-cmd-server"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except Exception:
        pass
    try:
        subprocess.run(
            ["killall", "-9", "mega-cmd"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except Exception:
        pass
    time.sleep(1)

    # 3. Очистка файла сессии (сбрасывает привязку к IP и квоту)
    session_file = Path.home() / ".megaCmd" / "session"
    try:
        if session_file.exists():
            session_file.unlink()
            add_log("PROXY: Файл сессии удалён")
    except Exception:
        pass

    # 4. Запуск демона
    try:
        subprocess.Popen(
            ["mega-cmd-server"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    except Exception:
        pass

    # 5. Ожидание готовности (до 10 секунд)
    for i in range(5):
        time.sleep(2)
        try:
            res = subprocess.run(
                ["mega-version"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=5,
            )
            if res.returncode == 0 and "stopped" not in res.stdout.lower():
                add_log("PROXY: mega-cmd-server перезапущен и готов")
                return
        except Exception:
            pass

    add_log("⚠️ mega-cmd-server не ответил после перезапуска", level="WARNING")


# ═══════════════════════════════════════════════════════════════════════════════
# Парсинг строк прокси
# ═══════════════════════════════════════════════════════════════════════════════

def _proxy_unique_key(p: dict) -> str:
    """
    Уникальный ключ прокси: host:port:username.
    Позволяет хранить несколько прокси с одинаковым host:port,
    но разными учётными записями (типично для ротационных прокси).
    """
    user = (p.get("username") or "").strip().lower()
    return f"{p['host'].lower()}:{p['port']}:{user}"


def parse_proxy_string(text: str) -> dict | None:
    """
    Разобрать строку прокси в словарь.

    Поддерживаемые форматы:
      - http://user:pass@host:port
      - socks5://host:port
      - host:port:user:pass
      - user:pass@host:port
      - host:port
    """
    text = text.strip()
    if not text or text.startswith("#"):
        return None

    protocol = "unknown"
    username = None
    password = None

    # 1. Схема
    if "://" in text:
        scheme, text = text.split("://", 1)
        protocol = scheme.lower().strip()

    # 2. user:pass@host:port
    if "@" in text:
        auth_part, text = text.rsplit("@", 1)
        if ":" in auth_part:
            username, password = auth_part.split(":", 1)
        else:
            username = auth_part

    # 3. host:port[:user:pass]
    pieces = text.split(":")
    if len(pieces) >= 4 and username is None:
        host = pieces[0].strip()
        try:
            port = int(pieces[1].strip())
        except ValueError:
            return None
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
        "id": uuid.uuid4().hex[:12],
        "host": host,
        "port": port,
        "protocol": protocol,
        "username": username or None,
        "password": password or None,
        "status": "untested",
        "ping": None,
        "error": None,
        "last_checked": None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Проверка доступности
# ═══════════════════════════════════════════════════════════════════════════════

def _build_requests_proxy_url(host: str, port: int, protocol: str,
                               username: str | None, password: str | None) -> str:
    """Собрать URL прокси для библиотеки requests (с URL-encoding учётных данных)."""
    scheme = "http"
    if protocol == "socks5":
        scheme = "socks5h"
    elif protocol == "socks4":
        scheme = "socks4a"

    if username and password:
        u = urllib.parse.quote(username, safe="")
        p = urllib.parse.quote(password, safe="")
        return f"{scheme}://{u}:{p}@{host}:{port}"
    return f"{scheme}://{host}:{port}"


def _test_single_protocol(host: str, port: int, protocol: str,
                           username: str | None, password: str | None,
                           timeout: float = 6.0) -> tuple[bool, int, str | None]:
    """
    Проверить один протокол через реальный HTTPS-запрос к API MEGA.
    Проверяет возможность HTTPS CONNECT туннелирования и доступность серверов MEGA.
    Возвращает (ok, latency_ms, error_msg).
    """
    import requests

    proxy_url = _build_requests_proxy_url(host, port, protocol, username, password)
    proxies = {"http": proxy_url, "https": proxy_url}
    # Проверяем реальное HTTPS CONNECT соединение к API MEGA
    test_url = "https://g.api.mega.co.nz/cs"

    t0 = time.time()
    try:
        resp = requests.get(test_url, proxies=proxies, timeout=timeout)
        latency = int((time.time() - t0) * 1000)
        # MEGA API возвращает 200 (или 204)
        if resp.status_code in (200, 204):
            return True, latency, None
        return False, latency, f"HTTP {resp.status_code}"
    except Exception as e:
        msg = str(e).lower()
        if "timeout" in msg or "timed out" in msg:
            return False, 0, "Таймаут HTTPS"
        if "connection refused" in msg:
            return False, 0, "Отказ в соединении"
        if "tunnel" in msg or "403" in msg or "407" in msg:
            return False, 0, "Ошибка HTTPS Tunnel"
        if "proxy" in msg or "socks" in msg:
            return False, 0, "Ошибка прокси"
        return False, 0, str(e)[:60]


def check_proxy(p: dict) -> dict:
    """Проверить доступность прокси и автоопределить протокол."""
    host = p["host"]
    port = p["port"]
    proto = p.get("protocol", "unknown")
    user = p.get("username")
    pwd = p.get("password")

    p["status"] = "checking"

    protocols = [proto] if proto != "unknown" else ["http", "socks5", "socks4"]
    err = None
    for pr in protocols:
        ok, latency, err = _test_single_protocol(host, port, pr, user, pwd, timeout=6.0)
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


# ═══════════════════════════════════════════════════════════════════════════════
# Менеджер пула прокси
# ═══════════════════════════════════════════════════════════════════════════════

class ProxyManager:
    """Потокобезопасный менеджер пула прокси для MEGAcmd."""

    def __init__(self):
        self._lock = threading.RLock()
        self.proxies: list[dict] = []
        self.active_proxy_id: str | None = None
        self.auto_rotate: bool = True
        # Счётчик ротаций в рамках одной задачи — сбрасывается извне
        self._rotation_attempts: int = 0
        self._scrape_lock = threading.Lock()
        self._last_auto_scrape_time: float = 0.0

    # ── Персистентность ───────────────────────────────────────────────────────

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
                        f, ensure_ascii=False, indent=2,
                    )
            except Exception:
                pass

    # ── Управление пулом ──────────────────────────────────────────────────────

    def add_proxies_text(self, text: str) -> list[dict]:
        """
        Разобрать многострочный текст и добавить новые прокси.
        Дедупликация по host:port:username — прокси с одинаковым IP но
        разными учётными записями считаются разными.
        """
        new_items = []
        with self._lock:
            existing_keys = {_proxy_unique_key(p) for p in self.proxies}
            for line in text.strip().splitlines():
                parsed = parse_proxy_string(line)
                if parsed:
                    key = _proxy_unique_key(parsed)
                    if key not in existing_keys:
                        self.proxies.append(parsed)
                        existing_keys.add(key)
                        new_items.append(parsed)
            self.save_to_disk()

        if new_items:
            threading.Thread(target=self.check_all, daemon=True).start()

        return new_items

    def check_all(self) -> None:
        """Проверить все прокси параллельно."""
        with self._lock:
            items = list(self.proxies)

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(check_proxy, items))

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
        """Удалить все неработающие прокси (offline, untested, checking)."""
        with self._lock:
            before = len(self.proxies)
            self.proxies = [p for p in self.proxies if p["status"] in ("online", "quota_exceeded")]
            self.save_to_disk()
            return before - len(self.proxies)

    def reset_quota_marks(self) -> int:
        """Сбросить все метки quota_exceeded обратно на online для повторной ротации."""
        count = 0
        with self._lock:
            for p in self.proxies:
                if p["status"] == "quota_exceeded":
                    p["status"] = "online"
                    p["error"] = None
                    count += 1
            self._rotation_attempts = 0
            self.save_to_disk()
        return count

    def _display_name(self, p: dict) -> str:
        """Человекочитаемое имя прокси для логов и UI."""
        user = p.get("username")
        if user:
            return f"{user}@{p['host']}:{p['port']}"
        return f"{p['host']}:{p['port']}"

    def get_state(self) -> dict:
        """Снимок состояния для веб-интерфейса."""
        with self._lock:
            proxies_out = []
            for p in self.proxies:
                entry = dict(p)
                entry["display_name"] = self._display_name(p)
                proxies_out.append(entry)
            return {
                "proxies": proxies_out,
                "active_proxy_id": self.active_proxy_id,
                "auto_rotate": self.auto_rotate,
                "count_total": len(self.proxies),
                "count_online": sum(1 for p in self.proxies if p["status"] == "online"),
            }

    # ── Управление MEGAcmd proxy ─────────────────────────────────────────────

    def apply_megacmd_proxy(self, proxy: dict, restart: bool = False) -> bool:
        """
        Применить прокси в MEGAcmd через команду mega-proxy.
        Логин/пароль передаются через флаги --username / --password.
        """
        if restart:
            restart_megacmd_server()
        else:
            ensure_megacmd_server_running()

        proto = (proxy.get("protocol") or "http").lower()
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

        name = self._display_name(proxy)
        add_log(f"PROXY: Применяю -> {name} ({proto.upper()})")

        try:
            res = subprocess.run(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, timeout=15,
            )
            if res.returncode == 0 or "PROXY_CUSTOM" in res.stdout:
                with self._lock:
                    self.active_proxy_id = proxy["id"]
                # Возобновляем активные передачи через новый прокси
                try:
                    subprocess.run(["mega-transfers", "-r", "-a"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
                except Exception:
                    pass
                add_log(f"✅ Прокси подключен: {name}")
                return True
            else:
                add_log(f"⚠️ mega-proxy ошибка: {res.stdout.strip()[:100]}", level="WARNING")
                return False
        except Exception as e:
            add_log(f"⚠️ mega-proxy исключение: {e}", level="WARNING")
            return False

    def disable_megacmd_proxy(self, restart: bool = False) -> None:
        """Отключить прокси в MEGAcmd (прямое соединение)."""
        if restart:
            restart_megacmd_server()
        else:
            ensure_megacmd_server_running()
        try:
            subprocess.run(
                ["mega-proxy", "--none"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=10,
            )
            subprocess.run(["mega-transfers", "-r", "-a"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except Exception:
            pass
        with self._lock:
            self.active_proxy_id = None
        add_log("PROXY: Отключен (прямое соединение)")

    # ── Ротация при квоте ─────────────────────────────────────────────────────

    def reset_rotation_counter(self) -> None:
        """Сбросить счётчик ротаций (вызывается перед каждой новой задачей)."""
        with self._lock:
            self._rotation_attempts = 0

    def rotate_on_quota(self, error_msg: str = "") -> bool:
        """
        Сменить прокси при исчерпании квоты MEGA «на лету» без перезапуска демона,
        чтобы сохранить состояние чанков для бесшовной докачки.
        """
        with self._lock:
            if not self.auto_rotate:
                return False

            max_attempts = len(self.proxies) + 1
            self._rotation_attempts += 1
            if self._rotation_attempts > max_attempts:
                add_log(
                    f"⚠️ Лимит ротации ({max_attempts}) достигнут. "
                    "Все прокси исчерпаны.",
                    level="WARNING",
                )
                self.disable_megacmd_proxy()
                return False

            # Помечаем текущий
            if self.active_proxy_id:
                for p in self.proxies:
                    if p["id"] == self.active_proxy_id:
                        p["status"] = "quota_exceeded"
                        p["error"] = (error_msg or "Квота MEGA")[:60]
                        break
                self.save_to_disk()

            # Ищем следующий online-прокси (не текущий!)
            next_p = self._pick_next_online_proxy()
            if not next_p:
                add_log(
                    "⚠️ Нет доступных online-прокси для ротации.",
                    level="WARNING",
                )
                self.disable_megacmd_proxy()
                return False

        name = self._display_name(next_p)
        add_log(f"🔄 Ротация -> {name}")
        return self.apply_megacmd_proxy(next_p, restart=False)

    def _pick_next_online_proxy(self) -> dict | None:
        """
        Выбрать следующий online-прокси, отличный от текущего активного.
        Если активного нет — вернуть первый online.
        """
        # Вызывается под self._lock
        online = [p for p in self.proxies if p["status"] == "online"]
        if not online:
            return None

        if not self.active_proxy_id:
            return online[0]

        # Найти позицию текущего и взять следующий
        current_idx = None
        for i, p in enumerate(online):
            if p["id"] == self.active_proxy_id:
                current_idx = i
                break

        if current_idx is not None:
            next_idx = (current_idx + 1) % len(online)
            candidate = online[next_idx]
            # Защита: если это тот же самый — значит он единственный online
            if candidate["id"] == self.active_proxy_id:
                return None
            return candidate

        # Текущий не найден среди online (уже помечен) — берём первый
        return online[0]

    # ── Поддержка нативного HTTP-клиента (requests) ──────────────────────────

    @staticmethod
    def build_requests_dict(proxy: dict | None) -> dict[str, str] | None:
        """Сформировать словарь proxies для requests из объекта прокси."""
        if not proxy:
            return None
        proto = (proxy.get("protocol") or "http").lower()
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

        if user and pwd:
            auth = f"{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@"
        elif user:
            auth = f"{urllib.parse.quote(user)}@"
        else:
            auth = ""

        proxy_url = f"{proto}://{auth}{host}:{port}"
        return {"http": proxy_url, "https": proxy_url}

    def get_available_proxies(self) -> list[dict]:
        """Получить список всех online-прокси."""
        with self._lock:
            return [dict(p) for p in self.proxies if p["status"] == "online"]

    def mark_proxy_quota(self, proxy_id: str, error_msg: str = "Квота MEGA") -> None:
        """Пометить прокси как исчерпавший квоту."""
        with self._lock:
            for p in self.proxies:
                if p["id"] == proxy_id:
                    p["status"] = "quota_exceeded"
                    p["error"] = error_msg[:60]
                    break
            self.save_to_disk()

    def mark_proxy_offline(self, proxy_id: str, error_msg: str = "Сбой соединения") -> None:
        """Пометить прокси как недоступный."""
        with self._lock:
            for p in self.proxies:
                if p["id"] == proxy_id:
                    p["status"] = "offline"
                    p["error"] = error_msg[:60]
                    break
            self.save_to_disk()

    def scrape_and_add_free_proxies(
        self,
        target_count: int = 30,
        progress_cb: Optional[Callable[[str], None]] = None,
    ) -> int:
        """
        Автоматически собрать бесплатные прокси из открытых источников,
        проверить их к MEGA API и добавить работающие в пул.
        """
        from .proxy_scraper import scrape_and_validate_proxies

        found = scrape_and_validate_proxies(target_count=target_count, progress_cb=progress_cb)
        added_count = 0

        with self._lock:
            existing_keys = {
                (p["host"], p["port"], p.get("username"))
                for p in self.proxies
            }
            for p in found:
                key = (p["host"], p["port"], p.get("username"))
                if key not in existing_keys:
                    existing_keys.add(key)
                    self.proxies.append(p)
                    added_count += 1

            if added_count > 0:
                self.save_to_disk()

        add_log(f"PROXY: В пул добавлено {added_count} новых бесплатных прокси", "OK")
        return added_count

    def ensure_working_proxies(self, min_count: int = 3, target_count: int = 35) -> list[dict]:
        """
        Гарантировать наличие доступных онлайн-прокси в пуле.
        Если онлайн-прокси меньше min_count, автоматически запускает сбор из 74+ источников.
        Потокобезопасно (выполняется только одним потоком одновременно).
        """
        available = self.get_available_proxies()
        if len(available) >= min_count:
            return available

        with self._scrape_lock:
            # Повторная проверка под блокировкой
            available = self.get_available_proxies()
            if len(available) >= min_count:
                return available

            now = time.time()
            # Пробуем сбросить метки квот, если они есть
            quota_proxies = [p for p in self.proxies if p["status"] == "quota_exceeded"]
            if quota_proxies and (now - self._last_auto_scrape_time < 90):
                add_log("♻️ Пробую повторно задействовать прокси из пула (сброс квот)...", "INFO")
                self.reset_quota_marks()
                available = self.get_available_proxies()
                if len(available) >= min_count:
                    return available

            # Запускаем авто-сбор из источников
            if now - self._last_auto_scrape_time > 30:
                add_log("🌐 Авто-поиск: квота исчерпана, собираю свежие прокси из 74+ источников...", "WARNING")
                self._last_auto_scrape_time = now
                self.scrape_and_add_free_proxies(target_count=target_count)

            return self.get_available_proxies()


# Глобальный синглтон
proxy_manager = ProxyManager()
