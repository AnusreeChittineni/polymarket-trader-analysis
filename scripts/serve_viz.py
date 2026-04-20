"""Tiny static file server for the D3+Canvas visualization.

Usage:
- python3 scripts/serve_viz.py 8000
Then open:
- http://127.0.0.1:8000/viz/

This serves the repo root so the viz can load:
- /viz/index.html
- /samples/trader_win_rate_by_category.csv
"""

from __future__ import annotations

import http.server
import os
import socketserver
import sys
from pathlib import Path


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    repo_root = Path(__file__).resolve().parents[1]

    os.chdir(repo_root)

    class NoCacheHandler(http.server.SimpleHTTPRequestHandler):
        def end_headers(self) -> None:  # type: ignore[override]
            # Avoid stale HTML/JS during iteration.
            self.send_header("Cache-Control", "no-store, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            super().end_headers()

    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    with ReusableTCPServer(("127.0.0.1", port), NoCacheHandler) as httpd:
        print(f"Serving {repo_root} at http://127.0.0.1:{port}/viz/")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
