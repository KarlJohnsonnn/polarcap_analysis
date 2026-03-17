#!/usr/bin/env python3
"""
Serve QuicklookBrowser with manifest refresh endpoints.

Usage:
    python web/cs_data_viewer/serve_quicklookbrowser.py
    python web/cs_data_viewer/serve_quicklookbrowser.py --repo-root /path/to/repo
"""

from __future__ import annotations

import argparse
import json
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from generate_manifest import generate_repo_manifest, get_default_repo_root, get_repo_status


class QuicklookBrowserServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_cls, repo_root: Path):
        super().__init__(server_address, handler_cls)
        self.repo_root = repo_root.resolve()


class QuicklookBrowserHandler(SimpleHTTPRequestHandler):
    server: QuicklookBrowserServer

    def translate_path(self, path: str) -> str:
        parsed = urlparse(path).path
        rel = parsed.lstrip("/")
        return str((self.server.repo_root / rel).resolve())

    def do_GET(self) -> None:
        parsed = urlparse(self.path).path
        if parsed == "/__quicklookbrowser/status":
            self.send_json(HTTPStatus.OK, {"ok": True, **get_repo_status(self.server.repo_root)})
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path).path
        if parsed != "/__quicklookbrowser/refresh-manifest":
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")
            return

        payload = self.read_json_body()
        repo_root = payload.get("repoRoot") if isinstance(payload, dict) else None
        if repo_root:
            requested_root = Path(repo_root).expanduser().resolve()
            if not requested_root.exists():
                self.send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": f"Repo root does not exist: {requested_root}"},
                )
                return
            self.server.repo_root = requested_root

        try:
            manifest_path = generate_repo_manifest(self.server.repo_root)
        except Exception as exc:  # pragma: no cover - defensive runtime response
            self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return

        self.send_json(
            HTTPStatus.OK,
            {"ok": True, "manifestPath": str(manifest_path), **get_repo_status(self.server.repo_root)},
        )

    def read_json_body(self) -> dict[str, object]:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        if content_length <= 0:
            return {}
        raw = self.rfile.read(content_length)
        return json.loads(raw.decode("utf-8")) if raw else {}

    def send_json(self, status: HTTPStatus, payload: dict[str, object]) -> None:
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to serve on.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=get_default_repo_root(),
        help="Repository root to serve and refresh.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.expanduser().resolve()
    server = QuicklookBrowserServer((args.host, args.port), QuicklookBrowserHandler, repo_root)
    print(f"QuicklookBrowser server at http://{args.host}:{args.port}")
    print(f"Repo root: {repo_root}")
    server.serve_forever()


if __name__ == "__main__":
    main()
