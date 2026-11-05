#!/usr/bin/env python3
"""
run_api.py
==========
Launch api:app using the port configured in .env / the environment.

`uvicorn api:app` ignores API_PORT — uvicorn's own --port flag (default 8000)
decides where the server binds, and api.py only uses API_PORT to build the CORS
allow-list. This launcher reads .env first and passes the resolved port to
uvicorn, so the two always agree.

Usage
-----
    python3 run_api.py                    # binds API_PORT (default 8000)
    python3 run_api.py --port 9000        # explicit override
    python3 run_api.py --reload           # auto-reload on code changes
    python3 run_api.py --host 0.0.0.0     # listen on all interfaces

Any other uvicorn option can be passed through, e.g. --workers 4.
"""

import argparse
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"


def load_dotenv(path: Path = ENV_FILE) -> None:
    """Load .env (real environment variables win)."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> int:
    load_dotenv()

    env_port = os.environ.get("API_PORT", "8000").strip() or "8000"
    env_host = os.environ.get("API_HOST", "127.0.0.1").strip() or "127.0.0.1"

    parser = argparse.ArgumentParser(
        description="Run api:app on the port from .env (API_PORT).",
    )
    parser.add_argument("--host", default=env_host,
                        help=f"Bind host (default: API_HOST or {env_host})")
    parser.add_argument("--port", type=int, default=int(env_port),
                        help=f"Bind port (default: API_PORT or {env_port})")
    parser.add_argument("--reload", action="store_true",
                        help="Auto-reload on code changes (development)")
    parser.add_argument("--log-level", default="info",
                        help="uvicorn log level (default: info)")
    args, extra = parser.parse_known_args()

    try:
        import uvicorn
    except ImportError:
        print("uvicorn is not installed. Run: pip3 install -r requirements_api.txt",
              file=sys.stderr)
        return 1

    # Keep the CORS allow-list in sync with the port we actually bind: api.py
    # builds it from API_PORT at import time, so override it when --port differs.
    if str(args.port) != os.environ.get("API_PORT", "").strip():
        os.environ["API_PORT"] = str(args.port)

    print(f"Starting api:app on http://{args.host}:{args.port}"
          f"  (API_PORT={os.environ.get('API_PORT', 'unset')})")

    uvicorn.run(
        "api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
        app_dir=str(BASE_DIR),
        *extra,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
