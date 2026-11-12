#!/usr/bin/env python3
"""Serve the feature derivation service.

    python3 -m feature_service.cli --db /tmp/feature.db --port 8080

The CLI prints one JSON line with the bound address, then serves until
interrupted.  Use `--port 0` to bind an ephemeral port.
"""
from __future__ import annotations

import argparse
import json
import sys

from .api import make_server
from .store import Store


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Feature-platform derivation service (JSON over HTTP)")
    parser.add_argument("--db", required=True, help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1",
                        help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8080,
                        help="bind port (default 8080, 0 = ephemeral)")
    args = parser.parse_args(argv)

    store = Store(args.db)
    try:
        server = make_server(store, args.host, args.port)
    except Exception:
        store.close()
        raise
    host, port = server.server_address[:2]
    print(json.dumps({"listening": f"http://{host}:{port}", "db": args.db}),
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.server_close()
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
