"""
Lightweight ``.env`` loader for DAG runtime configuration.

Secrets and environment-specific identifiers (e.g. Google Drive folder IDs) must
never be hardcoded in the committed DAGs. They are read at runtime from, in order
of precedence:

1. the real process environment (``os.environ``),
2. a ``.env`` file located at the repository root (gitignored),

so the same DAG code works in local, dev and prod without exposing private values.

This module has **no third-party dependencies** on purpose: the production base
image is built ahead of time and ``_PIP_ADDITIONAL_REQUIREMENTS`` is avoided, so a
hand-rolled parser is preferred over ``python-dotenv``.
"""

from __future__ import annotations

import os
from pathlib import Path

# Repository root: this file lives at ``<repo>/config/env.py``.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = _REPO_ROOT / ".env"

_loaded = False


def _load_dotenv_once() -> None:
    """
    Parse the repo-root ``.env`` once and populate ``os.environ``.

    Real environment variables always win: existing keys are never overwritten.
    Lines that are blank, comments (``#``) or malformed are ignored. ``export``
    prefixes and surrounding single/double quotes on values are stripped.
    """
    global _loaded
    if _loaded:
        return
    _loaded = True

    if not _ENV_PATH.is_file():
        return

    for raw in _ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :]

        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            # Real environment takes precedence over the .env file.
            os.environ.setdefault(key, value)


def get_env(name: str, default: str = "") -> str:
    """
    Return an environment variable, loading the repo ``.env`` on first use.

    Parameters
    ----------
    name : str
        Environment variable name.
    default : str, optional
        Value returned when the variable is unset (default: empty string).

    Returns
    -------
    str
        The resolved value, or ``default`` if not present.
    """
    _load_dotenv_once()
    return os.environ.get(name, default)
