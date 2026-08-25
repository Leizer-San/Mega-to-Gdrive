"""
image_compressor.py — Оптимизация и сжатие изображений перед архивацией/выгрузкой.
Настройки: макс. размер стороны 2160p, JPEG качество 92%, сжатие файлов > 1.0 MB и конвертация PNG/BMP/WEBP.
"""
from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Set, Tuple

from PIL import Image

from .helpers import add_log, format_bytes

MAX_IMAGE_DIMENSION: int = 2160
JPG_QUALITY: int = 92
MAX_FILE_SIZE_MB: float = 1.0

IMAGE_EXTENSIONS: Set[str] = frozenset([".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"])


def should_preserve_animation(image_path: Path) -> bool:
    """Проверяет, является ли изображение анимированным GIF/WEBP."""
    ext = image_path.suffix.lower()
    if ext == ".gif":
        return True
    if ext == ".webp":
        try:
            with Image.open(image_path) as image:
                return bool(getattr(image, "is_animated", False) or getattr(image, "n_frames", 1) > 1)
        except Exception:
            return True
    return False


def is_image_file(file_path: Path) -> bool:
    """Проверяет расширение файла на принадлежность к изображениям."""
    return file_path.suffix.lower() in IMAGE_EXTENSIONS


def process_image(image_path: Path) -> Path:
    """
    Обрабатывает одно изображение:
    - Пропускает анимации (GIF/анимированный WEBP).
    - Если размер > 1MB или формат PNG/BMP/WEBP:
      - Уменьшает разрешение, если меньшая сторона > 2160px.
      - Конвертирует в оптимизированный JPEG (качество 92%).
    """
    try:
        if not is_image_file(image_path):
            return image_path

        if should_preserve_animation(image_path):
            return image_path

        file_size_mb = image_path.stat().st_size / (1024 * 1024)
        needs_processing = file_size_mb > MAX_FILE_SIZE_MB

        with Image.open(image_path) as image:
            format_needs_conversion = (image.format or "").upper() in ["PNG", "BMP", "WEBP"]

            if needs_processing or format_needs_conversion:
                width, height = image.size
                max_dim = MAX_IMAGE_DIMENSION

                min_side = min(width, height)
                if min_side > max_dim:
                    ratio = max_dim / min_side
                    new_width = int(width * ratio)
                    new_height = int(height * ratio)
                    image = image.resize((new_width, new_height), Image.LANCZOS)

                new_image_path = image_path.with_suffix(".jpg")
                temp_path = image_path.with_name(f"temp_{new_image_path.name}")

                rgb_image = image.convert("RGB")
                rgb_image.save(temp_path, "JPEG", quality=JPG_QUALITY, optimize=True)

                if temp_path.exists():
                    if image_path.exists() and image_path != new_image_path:
                        image_path.unlink()
                    if new_image_path.exists() and new_image_path != temp_path:
                        new_image_path.unlink()
                    temp_path.rename(new_image_path)
                    return new_image_path

        return image_path
    except Exception as e:
        # При ошибке оставляем исходный файл
        return image_path


def compress_images_in_directory(
    target_dir: Path,
    max_workers: int = 8,
) -> Tuple[int, int, int]:
    """
    Рекурсивно находит и сжимает все изображения в target_dir в многопоточном режиме.
    Возвращает: (количество_обработанных, размер_до, размер_после)
    """
    all_imgs = [p for p in target_dir.rglob("*") if p.is_file() and is_image_file(p)]
    if not all_imgs:
        return 0, 0, 0

    size_before = sum(p.stat().st_size for p in all_imgs if p.exists())
    add_log(
        f"🖼️ Сжатие изображений: обработка {len(all_imgs)} файлов "
        f"({format_bytes(size_before)})...",
        "INFO",
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_image, p) for p in all_imgs]
        for f in as_completed(futures):
            try:
                f.result()
            except Exception:
                pass

    all_after = [p for p in target_dir.rglob("*") if p.is_file() and is_image_file(p)]
    size_after = sum(p.stat().st_size for p in all_after if p.exists())
    saved_bytes = max(0, size_before - size_after)
    saved_pct = (saved_bytes / size_before * 100) if size_before > 0 else 0

    add_log(
        f"✅ Сжатие завершено: {len(all_imgs)} файлов, {format_bytes(size_before)} ➔ "
        f"{format_bytes(size_after)} (сэкономлено {format_bytes(saved_bytes)}, -{saved_pct:.1f}%)",
        "OK",
    )
    return len(all_imgs), size_before, size_after
