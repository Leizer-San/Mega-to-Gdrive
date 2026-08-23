"""
tunnels.py — Настройка и отображение URL веб-туннелей.

Поддерживаемые варианты:
  1. Встроенный Colab-прокси
  2. Cloudflare Tunnel (cloudflared)
  3. Localtunnel (lt)
"""
import os
import time
import urllib.request


def setup_tunnels(port: int) -> None:
    """
    Запустить Cloudflare и Localtunnel в фоне,
    подождать их инициализации и вывести все доступные URL.
    """
    print(f"Запускаю туннели (Cloudflare, Localtunnel). Подождите 7-10 секунд...")

    # ── Cloudflare ────────────────────────────────────────────────────────────
    os.system(
        "wget -q -c -nc "
        "https://github.com/cloudflare/cloudflared/releases/latest/download/"
        "cloudflared-linux-amd64"
    )
    os.system("chmod +x cloudflared-linux-amd64")
    os.system(
        f"./cloudflared-linux-amd64 tunnel --url http://127.0.0.1:{port} "
        "> cloudflare.log 2>&1 &"
    )

    # ── Localtunnel ───────────────────────────────────────────────────────────
    os.system("npm install -g localtunnel -q > /dev/null 2>&1")
    os.system(f"lt --port {port} > lt.log 2>&1 &")

    time.sleep(7)

    # ── Читаем результаты ─────────────────────────────────────────────────────
    cf_url = _read_cloudflare_url()
    lt_url, lt_pass = _read_localtunnel_url()
    colab_url = _read_colab_url(port)

    _print_urls(colab_url, cf_url, lt_url, lt_pass)


def _read_cloudflare_url() -> str:
    try:
        with open("cloudflare.log", "r") as f:
            for line in f:
                if "trycloudflare.com" in line:
                    return "https://" + line.split("https://")[1].split()[0]
    except Exception:
        pass
    return "Не удалось получить URL"


def _read_localtunnel_url() -> tuple[str, str]:
    lt_url = "Не удалось получить URL"
    lt_pass = "Неизвестно"
    try:
        with open("lt.log", "r") as f:
            for line in f:
                if "your url is:" in line:
                    lt_url = line.split("is: ")[1].strip()
                    break
    except Exception:
        pass
    try:
        lt_pass = (
            urllib.request.urlopen("https://ipv4.icanhazip.com")
            .read()
            .decode("utf8")
            .strip()
        )
    except Exception:
        pass
    return lt_url, lt_pass


def _read_colab_url(port: int) -> str:
    try:
        from google.colab import output
        return output.eval_js(f"google.colab.kernel.proxyPort({port})")
    except Exception:
        return "Недоступно вне Colab"


def _print_urls(colab_url: str, cf_url: str, lt_url: str, lt_pass: str) -> None:
    sep = "=" * 74
    dash = "-" * 74
    print(sep)
    print("🌐 ВЕБ-ИНТЕРФЕЙС УСПЕШНО ЗАПУЩЕН!")
    print(sep)
    print(f"🔹 Вариант 1 (Встроенный Colab): {colab_url}")
    print("   * Работает только для вас в текущем браузере (быстро и безопасно).")
    print(dash)
    print(f"🔹 Вариант 2 (Cloudflare):       {cf_url}")
    print("   * Отличный вариант, чтобы открыть на телефоне или отправить ссылку.")
    print(dash)
    print(f"🔹 Вариант 3 (Localtunnel):      {lt_url}")
    print(f"   * Пароль (Endpoint IP) для входа на сайт: {lt_pass}")
    print(sep)
