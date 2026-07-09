import asyncio
import contextlib
import json
import os
import sys
from pathlib import Path

import anyio
from mcp import ClientSession
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage
from websockets.asyncio.server import serve

from langchain_mcp_adapters.reverse_ws import connect_reverse_websocket_relay


async def test_reverse_websocket_relay_exposes_local_stdio_mcp(
    socket_enabled,
    websocket_server_port: int,
):
    current_dir = Path(__file__).parent
    math_server_path = os.path.join(current_dir, "servers/math_server.py")
    done = anyio.Event()
    results = {}

    async def handle_cloud_connection(websocket):
        to_session_send, to_session_recv = anyio.create_memory_object_stream(100)
        from_session_send, from_session_recv = anyio.create_memory_object_stream(100)

        async def forward_cloud_to_session():
            async with to_session_send:
                async for raw_message in websocket:
                    await to_session_send.send(
                        SessionMessage(
                            JSONRPCMessage.model_validate(json.loads(raw_message))
                        )
                    )

        async def forward_session_to_relay():
            async with from_session_recv:
                async for session_message in from_session_recv:
                    await websocket.send(
                        json.dumps(
                            session_message.message.model_dump(
                                by_alias=True,
                                mode="json",
                                exclude_none=True,
                            )
                        )
                    )

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(forward_cloud_to_session)
            task_group.start_soon(forward_session_to_relay)

            async with ClientSession(to_session_recv, from_session_send) as session:
                await session.initialize()
                tools = await session.list_tools()
                result = await session.call_tool("add", {"a": 2, "b": 3})

            task_group.cancel_scope.cancel()

        results["tool_names"] = {tool.name for tool in tools.tools}
        results["content"] = [
            block.model_dump(mode="json", by_alias=True, exclude_none=True)
            for block in result.content
        ]
        done.set()

    async with serve(
        handle_cloud_connection,
        "127.0.0.1",
        websocket_server_port,
        subprotocols=["mcp"],
    ):
        relay_task = asyncio.create_task(
            connect_reverse_websocket_relay(
                url=f"ws://127.0.0.1:{websocket_server_port}",
                client_id="test-client",
                connections={
                    "math": {
                        "command": sys.executable,
                        "args": [math_server_path],
                        "transport": "stdio",
                    }
                },
            )
        )

        with anyio.fail_after(10):
            await done.wait()

        relay_task.cancel()
        with anyio.CancelScope(shield=True):
            with contextlib.suppress(asyncio.CancelledError):
                await relay_task

    assert results["tool_names"] == {"add", "multiply"}
    assert results["content"] == [{"type": "text", "text": "5"}]
