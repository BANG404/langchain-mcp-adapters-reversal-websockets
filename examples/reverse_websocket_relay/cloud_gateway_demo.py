"""Minimal cloud gateway demo for the reverse WebSocket MCP relay."""

# ruff: noqa: T201

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from typing import Protocol

import anyio
from mcp import ClientSession
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage, TextContent
from websockets.asyncio.server import serve


class _DemoWebSocket(Protocol):
    """Small protocol for the WebSocket methods this demo uses."""

    async def recv(self) -> str: ...

    async def send(self, message: str) -> None: ...


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the cloud gateway demo."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


async def main() -> None:
    """Run the cloud gateway demo until one relayed tool call completes."""
    args = parse_args()
    done = anyio.Event()

    async def handle_relay(websocket: _DemoWebSocket) -> None:
        hello = json.loads(await websocket.recv())
        print(
            "Relay connected: "
            f"client_id={hello['client_id']} servers={hello['servers']}"
        )

        session_id = "demo-session"
        server_name = "math"
        to_session_send, to_session_recv = anyio.create_memory_object_stream(100)
        from_session_send, from_session_recv = anyio.create_memory_object_stream(100)

        async def forward_relay_to_session() -> None:
            async with to_session_send:
                async for raw_message in websocket:
                    envelope = json.loads(raw_message)
                    if envelope["type"] != "mcp_message":
                        continue
                    await to_session_send.send(
                        SessionMessage(
                            JSONRPCMessage.model_validate(envelope["payload"])
                        )
                    )

        async def forward_session_to_relay() -> None:
            async with from_session_recv:
                async for session_message in from_session_recv:
                    payload = session_message.message.model_dump(
                        by_alias=True,
                        mode="json",
                        exclude_none=True,
                    )
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "mcp_message",
                                "session_id": session_id,
                                "server": server_name,
                                "payload": payload,
                            }
                        )
                    )

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(forward_relay_to_session)
            task_group.start_soon(forward_session_to_relay)

            async with ClientSession(to_session_recv, from_session_send) as session:
                await session.initialize()
                tools = await session.list_tools()
                tool_names = [tool.name for tool in tools.tools]
                print(f"Tools exposed through relay: {tool_names}")

                result = await session.call_tool("add", {"a": 2, "b": 3})
                content = result.content[0]
                if isinstance(content, TextContent):
                    print(f"add(2, 3) returned: {content.text}")
                else:
                    print(f"add(2, 3) returned non-text content: {content!r}")

            await websocket.send(
                json.dumps(
                    {
                        "type": "session_closed",
                        "session_id": session_id,
                        "server": server_name,
                    }
                )
            )
            task_group.cancel_scope.cancel()

        done.set()

    async with serve(handle_relay, args.host, args.port, subprotocols=["mcp-reverse"]):
        print(f"Cloud gateway listening on ws://{args.host}:{args.port}")
        await done.wait()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
