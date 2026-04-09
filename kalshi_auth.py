"""
Kalshi API authentication — RSA-PSS request signing.
Generates the three required headers for every authenticated request.
"""
from __future__ import annotations

import base64
import time
from pathlib import Path

import config


def _load_private_key():
    """Load RSA private key from PEM file."""
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    key_path = config.KALSHI_PRIVATE_KEY_PATH
    if not key_path:
        return None
    pem_data = Path(key_path).read_bytes()
    return load_pem_private_key(pem_data, password=None)


def sign_request(method: str, path: str) -> dict[str, str]:
    """
    Generate Kalshi auth headers for a request.

    Args:
        method: HTTP method (GET, POST, DELETE, etc.)
        path: Request path (e.g. /trade-api/v2/markets)

    Returns:
        Dict with KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP headers.
    """
    from cryptography.hazmat.primitives.asymmetric.padding import PSS, MGF1
    from cryptography.hazmat.primitives.hashes import SHA256

    private_key = _load_private_key()
    if not private_key:
        raise ValueError("Kalshi private key not configured (set KALSHI_PRIVATE_KEY_PATH)")

    timestamp_ms = str(int(time.time() * 1000))
    message = f"{timestamp_ms}{method.upper()}{path}"

    signature = private_key.sign(
        message.encode(),
        PSS(mgf=MGF1(SHA256()), salt_length=SHA256.digest_size),
        SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": config.KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }


def auth_headers(method: str, path: str) -> dict[str, str]:
    """Convenience wrapper — returns empty dict if no key configured."""
    if not config.KALSHI_API_KEY_ID or not config.KALSHI_PRIVATE_KEY_PATH:
        return {}
    return sign_request(method, path)
