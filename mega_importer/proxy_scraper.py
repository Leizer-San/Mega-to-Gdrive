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

# Полный список 74+ проверенных источников прокси (на базе MDPR)
PROXY_SOURCES = [
    # --- HTML scraping ---
    {"name": "free-proxy-list", "url": "https://free-proxy-list.net/", "protocol": "http"},
    {"name": "sslproxies", "url": "https://www.sslproxies.org/", "protocol": "http"},
    {"name": "us-proxy", "url": "https://www.us-proxy.org/", "protocol": "http"},
    {"name": "free-proxy-list-anonymous", "url": "https://free-proxy-list.net/anonymous-proxy.html", "protocol": "http"},

    # --- API plain text ---
    {"name": "proxyscrape-http", "url": "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all", "protocol": "http"},
    {"name": "proxy-list-download-http", "url": "https://www.proxy-list.download/api/v1/get?type=http", "protocol": "http"},
    {"name": "openproxylist-http", "url": "https://api.openproxylist.xyz/http.txt", "protocol": "http"},
    {"name": "proxyspace-http", "url": "https://proxyspace.pro/http.txt", "protocol": "http"},

    # --- GitHub HTTP / HTTPS repositories ---
    {"name": "thespeedx-http", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", "protocol": "http"},
    {"name": "monosans-http", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "clarketm-proxy-list", "url": "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "protocol": "http"},
    {"name": "jetkai-http", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt", "protocol": "http"},
    {"name": "mmpx12-http", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt", "protocol": "http"},
    {"name": "proxifly-http", "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt", "protocol": "http"},
    {"name": "gfpcom-http", "url": "https://raw.githubusercontent.com/gfpcom/free-proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "vakhov-http", "url": "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt", "protocol": "http"},
    {"name": "proxygenerator1-http", "url": "https://raw.githubusercontent.com/proxygenerator1/ProxyGenerator/main/http_proxies.txt", "protocol": "http"},
    {"name": "shiftytr-http", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt", "protocol": "http"},
    {"name": "roosterkid-https", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt", "protocol": "http"},
    {"name": "murongpig-http", "url": "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt", "protocol": "http"},
    {"name": "zloi-user-http", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/master/http.txt", "protocol": "http"},
    {"name": "b4rc0de-http", "url": "https://raw.githubusercontent.com/B4RC0DE-TM/proxy-list/main/HTTP.txt", "protocol": "http"},
    {"name": "proxy4parsing-http", "url": "https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt", "protocol": "http"},
    {"name": "almroot-http", "url": "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt", "protocol": "http"},
    {"name": "spys-me-http", "url": "https://spys.me/proxy.txt", "protocol": "http"},
    {"name": "fate0-proxylist", "url": "https://raw.githubusercontent.com/fate0/proxylist/master/proxy.list", "protocol": "http"},
    {"name": "jetkai-https", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt", "protocol": "http"},
    {"name": "shiftytr-https", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt", "protocol": "http"},
    {"name": "mmpx12-https", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/https.txt", "protocol": "http"},
    {"name": "vakhov-https", "url": "https://vakhov.github.io/fresh-proxy-list/https.txt", "protocol": "http"},
    {"name": "vpslab-http-all", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt", "protocol": "http"},
    {"name": "vpslab-http-ssl", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_ssl.txt", "protocol": "http"},
    {"name": "vpslab-http-elite", "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_elite.txt", "protocol": "http"},
    {"name": "vmheaven-http", "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/http.txt", "protocol": "http"},
    {"name": "vmheaven-https", "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/main/https.txt", "protocol": "http"},
    {"name": "komutan234-http", "url": "https://raw.githubusercontent.com/komutan234/Proxy-List-Free/main/proxies/http.txt", "protocol": "http"},
    {"name": "rdavydov-http", "url": "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "zevtyardt-http", "url": "https://raw.githubusercontent.com/zevtyardt/proxy-list/main/http.txt", "protocol": "http"},
    {"name": "kangproxy-http", "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/master/http.txt", "protocol": "http"},
    {"name": "thordata-http", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "thordata-https", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/https.txt", "protocol": "http"},
    {"name": "zaeem20-http", "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt", "protocol": "http"},
    {"name": "zaeem20-https", "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt", "protocol": "http"},
    {"name": "ercindedeoglu-http", "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/http.txt", "protocol": "http"},
    {"name": "yemixzy-http", "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/http.txt", "protocol": "http"},
    {"name": "proxygenerator1-stable-http", "url": "https://raw.githubusercontent.com/proxygenerator1/ProxyGenerator/main/Stable/http.txt", "protocol": "http"},
    {"name": "prx-chk-http", "url": "https://raw.githubusercontent.com/prx-chk/proxy-list/main/http.txt", "protocol": "http"},

    # --- JSON Mirrors ---
    {"name": "proxyscrape-gh-http", "url": "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/http/data.json", "protocol": "http"},
    {"name": "proxyscrape-gh-socks5", "url": "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/socks5/data.json", "protocol": "socks5"},
    {"name": "proxyscrape-gh-socks4", "url": "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/protocols/socks4/data.json", "protocol": "socks4"},

    # --- SOCKS5 / SOCKS4 repositories ---
    {"name": "thespeedx-socks5", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "protocol": "socks5"},
    {"name": "monosans-socks5", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    {"name": "hookzof-socks5", "url": "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt", "protocol": "socks5"},
    {"name": "shiftytr-socks5", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt", "protocol": "socks5"},
    {"name": "jetkai-socks5", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt", "protocol": "socks5"},
    {"name": "roosterkid-socks5", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt", "protocol": "socks5"},
    {"name": "mmpx12-socks5", "url": "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "protocol": "socks5"},
    {"name": "vakhov-socks5", "url": "https://vakhov.github.io/fresh-proxy-list/socks5.txt", "protocol": "socks5"},
    {"name": "zloi-user-socks5", "url": "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt", "protocol": "socks5"},
    {"name": "rdavydov-socks5", "url": "https://raw.githubusercontent.com/rdavydov/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    {"name": "zaeem20-socks5", "url": "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt", "protocol": "socks5"},
    {"name": "ercindedeoglu-socks5", "url": "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/socks5.txt", "protocol": "socks5"},
    {"name": "thordata-socks5", "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    {"name": "yemixzy-socks5", "url": "https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/socks5.txt", "protocol": "socks5"},
    {"name": "proxifly-socks5", "url": "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.txt", "protocol": "socks5"},
    {"name": "prx-chk-socks5", "url": "https://raw.githubusercontent.com/prx-chk/proxy-list/main/socks5.txt", "protocol": "socks5"},
    {"name": "proxyscrape-socks5", "url": "https://api.proxyscrape.com/v2/?request=getproxies&protocol=socks5&timeout=5000&country=all", "protocol": "socks5"},
    {"name": "proxyspace-socks5", "url": "https://proxyspace.pro/socks5.txt", "protocol": "socks5"},
    {"name": "openproxylist-socks5", "url": "https://api.openproxylist.xyz/socks5.txt", "protocol": "socks5"},
    {"name": "thespeedx-socks4", "url": "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "protocol": "socks4"},
    {"name": "monosans-socks4", "url": "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "protocol": "socks4"},
    {"name": "shiftytr-socks4", "url": "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt", "protocol": "socks4"},
    {"name": "roosterkid-socks4", "url": "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt", "protocol": "socks4"},
    {"name": "jetkai-socks4", "url": "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt", "protocol": "socks4"},
]

IP_PORT_RE = re.compile(r"\b((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]{2,5})\b")
MEGA_TEST_URL = "https://g.api.mega.co.nz/cs"


def _fetch_source(source: dict) -> List[dict]:
    """Fetch and parse ip:port lines or JSON entries from a single source."""
    proto = source["protocol"]
    candidates = []
    try:
        resp = requests.get(
            source["url"],
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        )
        if resp.status_code == 200:
            text = resp.text
            # If JSON array of objects
            if text.startswith("[") or text.startswith("{"):
                try:
                    data = json.loads(text)
                    items = data if isinstance(data, list) else data.get("data", [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict):
                                ip = item.get("ip") or item.get("host")
                                port = item.get("port")
                                if ip and port:
                                    candidates.append({
                                        "host": str(ip).strip(),
                                        "port": int(port),
                                        "protocol": proto,
                                        "source": source["name"],
                                    })
                except Exception:
                    pass

            # Regex search for all standard IP:PORT matches in response text/HTML
            for match in IP_PORT_RE.finditer(text):
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

    scheme = "socks5h" if proto == "socks5" else "socks4a" if proto == "socks4" else "http"
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
    target_count: int = 50,
    max_test_candidates: int = 800,
    progress_cb: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    """
    Fetch public proxies from 74+ sources, deduplicate, and validate against MEGA API in parallel.
    Stops as soon as target_count working proxies are found.
    """
    if progress_cb:
        progress_cb("Сбор списков прокси из 74+ публичных источников...")
    add_log(f"SCRAPER: Начинаю параллельный сбор прокси из {len(PROXY_SOURCES)} источников...", "INFO")

    raw_candidates: List[dict] = []
    seen = set()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(_fetch_source, s) for s in PROXY_SOURCES]
        for f in as_completed(futures):
            for c in f.result():
                key = (c["host"], c["port"])
                if key not in seen:
                    seen.add(key)
                    raw_candidates.append(c)

    total_scraped = len(raw_candidates)
    add_log(f"SCRAPER: Собрано {total_scraped} уникальных прокси. Запуск быстрой валидации к MEGA API...", "INFO")

    if not raw_candidates:
        return []

    # Limit testing pool to max_test_candidates
    test_pool = raw_candidates[:max_test_candidates]
    working_proxies: List[dict] = []

    if progress_cb:
        progress_cb(f"Проверка {len(test_pool)} прокси к MEGA API...")

    with ThreadPoolExecutor(max_workers=50) as executor:
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
