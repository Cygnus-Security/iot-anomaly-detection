"""Streamlit HTTP readiness probe used by Docker Compose."""

from __future__ import annotations

from urllib.error import URLError
from urllib.request import urlopen


def main() -> int:
    try:
        with urlopen("http://127.0.0.1:8501/_stcore/health", timeout=2) as response:
            return 0 if response.status == 200 else 1
    except (OSError, URLError):
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
