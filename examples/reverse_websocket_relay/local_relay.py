"""Local reverse WebSocket relay demo.

Run this after starting `cloud_gateway_demo.py`.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from langchain_mcp_adapters.reverse_ws import (
    connect_reverse_websocket_relay,
    run_reverse_websocket_relay,
)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the local relay demo."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://127.0.0.1:8765")
    parser.add_argument("--client-id", default="local-demo")
    parser.add_argument("--token", default=None)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Connect once and exit when the gateway closes the WebSocket.",
    )
    return parser.parse_args()


async def main() -> None:
    """Run the local reverse WebSocket relay demo."""
    args = parse_args()
    math_server = Path(__file__).with_name("math_server.py")
    connections = {
        "math": {
            "transport": "stdio",
            "command": "python3",
            "args": [str(math_server)],
        }
    }

    if args.once:
        await connect_reverse_websocket_relay(
            url=args.url,
            client_id=args.client_id,
            token=args.token,
            connections=connections,
        )
        return

    await run_reverse_websocket_relay(
        url=args.url,
        client_id=args.client_id,
        token=args.token,
        connections=connections,
    )


if __name__ == "__main__":
    asyncio.run(main())
