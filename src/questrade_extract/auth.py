"""Questrade OAuth token management.

Reads ~/.config/questrade/token, exchanges the refresh token for a new
access token, and writes the rotated refresh token back immediately.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_TOKEN_URL = "https://login.questrade.com/oauth2/token"
_DEFAULT_TOKEN_FILE = Path.home() / ".config" / "questrade" / "token"

_RESEED_INSTRUCTIONS = """
[ERROR] Questrade token rejected. Your refresh token has expired.

To re-seed:
  1. Log in to Questrade → top-right menu → API centre
  2. Click your app → New manual authorization → Generate new token → Copy
  3. Run:
       curl -s 'https://login.questrade.com/oauth2/token?grant_type=refresh_token&refresh_token=PASTE_TOKEN_HERE'
  4. Save the response:
       echo '<paste JSON here>' > ~/.config/questrade/token
"""


class AuthError(Exception):
    pass


def _token_file(path: str | None) -> Path:
    return Path(path) if path else _DEFAULT_TOKEN_FILE


def refresh(token_file_path: str | None = None) -> tuple[str, str]:
    """Exchange the stored refresh token for a fresh access token.

    Returns (access_token, api_server). Rotates the refresh token on disk.
    Raises AuthError with clear re-seed instructions on 400/401.
    """
    path = _token_file(token_file_path)

    try:
        stored = json.loads(path.read_text())
    except FileNotFoundError:
        raise AuthError(
            f"Token file not found at {path}.\n"
            "Create it with: mkdir -p ~/.config/questrade && "
            "echo '{\"refresh_token\": \"...\", \"api_server\": \"https://api01.iq.questrade.com/\"}'"
            f" > {path}"
        )

    refresh_token = stored.get("refresh_token")
    if not refresh_token:
        raise AuthError(f"No refresh_token found in {path}")

    try:
        resp = requests.get(
            _TOKEN_URL,
            params={"grant_type": "refresh_token", "refresh_token": refresh_token},
            timeout=15,
        )
    except requests.RequestException as e:
        raise AuthError(f"Network error contacting Questrade auth: {e}") from e

    if resp.status_code in (400, 401):
        raise AuthError(_RESEED_INSTRUCTIONS)

    resp.raise_for_status()
    data = resp.json()

    # Rotate — write new token state back immediately
    path.write_text(json.dumps(data))
    logger.debug("Token rotated and saved to %s", path)

    return data["access_token"], data["api_server"].rstrip("/")
