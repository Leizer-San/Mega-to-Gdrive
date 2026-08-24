"""
MEGA cryptographic primitives and key derivation functions.
Based on standard MEGA API specification & MDPR.
"""
from __future__ import annotations

import base64
import json
import struct
from typing import Tuple

from Crypto.Cipher import AES
from Crypto.Util import Counter


def base64_url_decode(data: str) -> bytes:
    """Decode base64url string (with or without padding, handling legacy formats)."""
    data = data.replace("-", "+").replace("_", "/").replace(",", "")
    data += "=" * ((4 - len(data) % 4) % 4)
    return base64.b64decode(data)


def base64_url_encode(data: bytes) -> str:
    """Encode bytes into unpadded base64url string."""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def a32_to_str(a: Tuple[int, ...] | list[int]) -> bytes:
    """Convert tuple/list of 32-bit unsigned integers to bytes (big-endian)."""
    return struct.pack(f">{len(a)}I", *a)


def str_to_a32(b: bytes) -> Tuple[int, ...]:
    """Convert bytes to tuple of 32-bit unsigned integers (big-endian)."""
    if len(b) % 4:
        b += b"\0" * (4 - len(b) % 4)
    return struct.unpack(f">{len(b) // 4}I", b)


def base64_to_a32(s: str) -> Tuple[int, ...]:
    """Decode base64url string directly into tuple of 32-bit integers."""
    return str_to_a32(base64_url_decode(s))


def a32_to_base64(a: Tuple[int, ...] | list[int]) -> str:
    """Convert tuple of 32-bit integers to unpadded base64url string."""
    return base64_url_encode(a32_to_str(a))


def decrypt_attr(data: bytes, key: Tuple[int, ...]) -> dict | None:
    """
    Decrypt MEGA attributes blob (AES-CBC, IV=0).
    Returns dict if valid JSON, otherwise None.
    """
    try:
        if len(key) == 8:
            k = (key[0] ^ key[4], key[1] ^ key[5], key[2] ^ key[6], key[3] ^ key[7])
        elif len(key) == 4:
            k = key
        else:
            return None

        cipher = AES.new(a32_to_str(k), AES.MODE_CBC, b"\0" * 16)
        decrypted = cipher.decrypt(data)
        if not decrypted.startswith(b"MEGA"):
            return None

        text = decrypted[4:].decode("utf-8", errors="replace")
        # Use raw_decode to ignore trailing padding
        decoder = json.JSONDecoder()
        obj, _ = decoder.raw_decode(text.lstrip("\0"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def decrypt_key(a: Tuple[int, ...], key: Tuple[int, ...]) -> Tuple[int, ...]:
    """Decrypt a node key using master key (AES-ECB in 4-word chunks)."""
    cipher = AES.new(a32_to_str(key), AES.MODE_ECB)
    raw = a32_to_str(a)
    decrypted = cipher.decrypt(raw)
    return str_to_a32(decrypted)


def derive_file_key(key_a32: Tuple[int, ...]) -> tuple[bytes, int]:
    """
    Derive AES-128 key (16 bytes) and initial 128-bit counter integer from MEGA 8-word key.
    """
    if len(key_a32) < 8:
        # Fallback for 4-word keys
        k = key_a32[:4]
        iv = (0, 0, 0, 0)
    else:
        k = (
            key_a32[0] ^ key_a32[4],
            key_a32[1] ^ key_a32[5],
            key_a32[2] ^ key_a32[6],
            key_a32[3] ^ key_a32[7],
        )
        iv = (key_a32[4], key_a32[5], 0, 0)

    key_bytes = a32_to_str(k)
    # IV as 128-bit integer: (iv[0] << 96) | (iv[1] << 64)
    iv_int = (iv[0] << 96) | (iv[1] << 64) | (iv[2] << 32) | iv[3]
    return key_bytes, iv_int


def create_aes_ctr_cipher(key_bytes: bytes, initial_counter_int: int, byte_offset: int = 0) -> AES.AESCipher:
    """
    Create AES-CTR cipher instance positioned at the given byte_offset.
    byte_offset MUST be a multiple of 16 (AES block size).
    """
    block_offset = byte_offset // 16
    current_counter = (initial_counter_int + block_offset) & ((1 << 128) - 1)
    counter = Counter.new(128, initial_value=current_counter)
    return AES.new(key_bytes, AES.MODE_CTR, counter=counter)
