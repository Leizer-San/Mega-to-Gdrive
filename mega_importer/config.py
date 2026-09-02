"""
config.py — Все константы и пути проекта.
"""
from pathlib import Path

# ── Сервер ────────────────────────────────────────────────────────────────────
PORT = 7860

# ── Рабочие директории (временные, в Colab-окружении) ────────────────────────
WORK_DIR     = Path("/content/mega_importer")
DOWNLOAD_DIR = WORK_DIR / "downloads"

# ── Постоянное хранилище на Google Drive ─────────────────────────────────────
PERSISTENT_DIR = Path("/content/drive/MyDrive/MegaImporter_State")
STATE_FILE     = PERSISTENT_DIR / "state.json"
PROXIES_FILE   = PERSISTENT_DIR / "proxies.json"

# ── Параметры загрузки ────────────────────────────────────────────────────────
RESERVE_BYTES      = 5   * 1024 ** 3   # 5 GB — резерв свободного места на Drive
UPLOAD_CHUNK       = 100 * 1024 ** 2   # 100 MB — размер чанка resumable-upload
MAX_RETRIES        = 3                  # Максимальное количество повторных попыток
DEFAULT_BATCH_SIZE = int(3.5 * 1024 ** 3)  # 3.5 GB — адаптивный сегмент (наполнение ~2.5 - 3.5 GB)

# ── Pixeldrain ───────────────────────────────────────────────────────────────
import os
PIXELDRAIN_API_KEY = os.environ.get("PIXELDRAIN_API_KEY", "")


