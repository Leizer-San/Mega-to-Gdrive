"""
proxy_scraper.py — Auto-scraper and validator for free public proxies.
Fetches fresh HTTP/SOCKS5 proxies from 15+ curated repositories and validates them against MEGA API.
"""
from __future__ import annotations

import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, List, Optional

import requests

from .helpers import add_log

# Curated high-yield proxy sources
PROXY_SOURCES = [
    {"name": "TheSpeedX-HTTP", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "protocol": "http"},
    {"name": "TheSpeedX-SOCKS5", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "protocol": "socks5"},
    {"name": "monosans-HTTP", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "monosans-SOCKS5", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    {"name": "ProxyScrape-HTTP", "url": "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=5000&country=all", "protocol": "http"},
    {"name": "ProxyScrape-SOCKS5", "url": "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=5000&country=all", "protocol": "socks5"},
    {"name": "ProxySpace-HTTP", "url": "https://proxyspace.pro/http.txt", "protocol": "http"},
    {"name": "ProxySpace-SOCKS5", "url": "https://proxyspace.pro/socks5.txt", "protocol": "socks5"},
    {"name": "OpenProxyList-HTTP", "url": "https://api.openproxylist.xyz/http.txt", "protocol": "http"},
    {"name": "OpenProxyList-SOCKS5", "url": "https://api.openproxylist.xyz/socks5.txt", "protocol": "socks5"},
    {"name": "hookzof-SOCKS5", "url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "protocol": "socks5"},
    {"name": "roosterkid-SOCKS5", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "protocol": "socks5"},
    {"name": "roosterkid-HTTPS", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "protocol": "http"},
    {"name": "prx-chk-HTTP", "url": "https://raw.githubusercontent.com/prx-chk/proxy-list/main/http.txt", "protocol": "http"},
    {"name": "prx-chk-SOCKS5", "url": "https://raw.githubusercontent.com/prx-chk/proxy-list/main/socks5.txt", "protocol": "socks5"},
]

IP_PORT_RE = re.compile(r"\b((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]{2,5})\b")
MEGA_TEST_URL = "https://g.api.mega.co.nz/cs"


def _fetch_source(source: dict) -> List[dict]:
    """Fetch and parse ip:port lines from a single source."""
    proto = source["protocol"]
    candidates = []
    try:
        resp = requests.get(source["url"], timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            for match in IP_PORT_RE.finditer(resp.text):
                host, port_str = match.group(1), match.group(2)
                port = int(port_str)
                if 1 <= port <= 65535:
                    candidates.append({
                        "host": host,
                        "port": port,
                        "protocol": proto,
                        "source": source["name"],
                    })
    except Exception:
        pass
    return candidates


def _test_proxy_mega(candidate: dict, timeout: float = 4.5) -> Optional[dict]:
    """
    Test a proxy against MEGA API endpoint via HTTPS CONNECT tunnel.
    Returns populated proxy dict if online, else None.
    """
    proto = candidate["protocol"]
    host = candidate["host"]
    port = candidate["port"]

    scheme = "socks5h" if proto == "socks5" else "http"
    proxy_url = f"{scheme}://{host}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}

    t0 = time.time()
    try:
        resp = requests.post(
            MEGA_TEST_URL,
            json=[{"a": "g"}],
            proxies=proxies,
            timeout=timeout,
        )
        latency = int((time.time() - t0) * 1000)
        # Any response from MEGA API means the proxy is routing HTTPS correctly
        if resp.status_code in (200, 400, 403, 500, 509):
            return {
                "id": uuid.uuid4().hex[:8],
                "host": host,
                "port": port,
                "protocol": proto,
                "username": None,
                "password": None,
                "display_name": f"{host}:{port} ({proto.upper()})",
                "status": "online",
                "ping": latency,
                "error": None,
                "added_at": int(time.time()),
            }
    except Exception:
        pass
    return None


def scrape_and_validate_proxies(
    target_count: int = 30,
    max_test_candidates: int = 400,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    """
    Fetch public proxies, deduplicate, and validate against MEGA API in parallel.
    Stops as soon as target_count working proxies are found.
    """
    if progress_cb:
        progress_cb("Сбор списков прокси из публичных источников...")
    add_log("SCRAPER: Начинаю сбор бесплатных прокси из 15+ источников...", "INFO")

    raw_candidates: List[dict] = []
    seen = set()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_source, s) for s in PROXY_SOURCES]
        for f in as_completed(futures):
            for c in f.result():
                key = (c["host"], c["port"])
                if key not in seen:
                    seen.add(key)
                    raw_candidates.append(c)

    total_scraped = len(raw_candidates)
    add_log(f"SCRAPER: Собрано {total_scraped} уникальных прокси. Запуск валидации к MEGA API...", "INFO")

    if not raw_candidates:
        return []

    # Limit testing pool to max_test_candidates
    test_pool = raw_candidates[:max_test_candidates]
    working_proxies: List[dict] = []

    if progress_cb:
        progress_cb(f"Проверка {len(test_pool)} прокси к MEGA API...")

    with ThreadPoolExecutor(max_workers=35) as executor:
        futures = {executor.submit(_test_proxy_mega, cand): cand for cand in test_pool}
        tested_count = 0

        for f in as_completed(futures):
            tested_count += 1
            res = f.result()
            if res:
                working_proxies.append(res)
                add_log(f"✅ Найден рабочий прокси: {res['display_name']} ({res['ping']} ms)", "OK")

                if len(working_proxies) >= target_count:
                    # Cancel remaining
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

            if tested_count % 50 == 0 and progress_cb:
                progress_cb(f"Проверено {tested_count}/{len(test_pool)}: найдено {len(working_proxies)} онлайн")

    # Sort by ping
    working_proxies.sort(key=lambda p: p.get("ping") or 9999)
    add_log(f"SCRAPER: Готово! Найдено {len(working_proxies)} рабочих прокси для MEGA", "OK")
    return working_proxies
