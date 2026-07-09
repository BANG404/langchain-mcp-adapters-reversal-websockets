"""Reverse WebSocket transport for exposing a local MCP server to cloud agents.

The local process initiates the WebSocket connection, but the messages on that
connection are standard MCP JSON-RPC messages without a relay envelope.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, Self, cast

import anyio
import httpx
from mcp import StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage

from langchain_mcp_adapters.sessions import (
    DEFAULT_ENCODING,
    DEFAULT_ENCODING_ERROR_HANDLER,
    DEFAULT_HTTP_TIMEOUT,
    DEFAULT_SSE_READ_TIMEOUT,
    DEFAULT_STREAMABLE_HTTP_SSE_READ_TIMEOUT,
    DEFAULT_STREAMABLE_HTTP_TIMEOUT,
    Connection,
    McpHttpClientFactory,
    _expand_env_vars,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from types import TracebackType

    from anyio.streams.memory import (
        MemoryObjectReceiveStream,
        MemoryObjectSendStream,
    )
    from websockets.typing import Subprotocol

logger = logging.getLogger(__name__)

DEFAULT_RECONNECT_INTERVAL = 5.0


class _WebSocket(Protocol):
    def __aiter__(self) -> AsyncIterator[str | bytes]: ...

    async def send(self, message: str) -> None: ...


class ReverseWebSocketRelayError(Exception):
    """Error raised by the reverse WebSocket relay."""


class _DirectMcpBridge:
    def __init__(
        self,
        *,
        connection: Connection,
        websocket: _WebSocket,
    ) -> None:
        self.connection = connection
        self.websocket = websocket
        self._exit_stack = AsyncExitStack()
        self._write_stream: MemoryObjectSendStream[SessionMessage] | None = None
        self._task_group: anyio.abc.TaskGroup | None = None
        self._closed = anyio.Event()

    async def __aenter__(self) -> Self:
        read_stream, write_stream = await self._exit_stack.enter_async_context(
            _connect_local_transport(self.connection)
        )
        self._write_stream = write_stream
        task_group = await self._exit_stack.enter_async_context(
            anyio.create_task_group()
        )
        self._task_group = task_group
        task_group.start_soon(self._forward_local_messages, read_stream)
        task_group.start_soon(self._forward_websocket_messages)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._task_group is not None:
            self._task_group.cancel_scope.cancel()
        await self._exit_stack.aclose()

    async def wait_closed(self) -> None:
        """Wait until either side of the MCP bridge closes."""
        await self._closed.wait()

    async def _send_to_local(self, payload: dict[str, Any]) -> None:
        if self._write_stream is None:
            msg = "Reverse WebSocket MCP bridge is not open"
            raise ReverseWebSocketRelayError(msg)
        message = JSONRPCMessage.model_validate(payload)
        await self._write_stream.send(SessionMessage(message=message))

    async def _forward_websocket_messages(self) -> None:
        try:
            async for raw_message in self.websocket:
                payload = _decode_json_rpc(raw_message)
                await self._send_to_local(payload)
        finally:
            if self._task_group is not None:
                self._task_group.cancel_scope.cancel()
            self._closed.set()

    async def _forward_local_messages(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        try:
            async with read_stream:
                async for item in read_stream:
                    if isinstance(item, Exception):
                        logger.exception(
                            "Local MCP transport emitted an error",
                            exc_info=item,
                        )
                        continue

                    await self.websocket.send(
                        json.dumps(
                            item.message.model_dump(
                                by_alias=True,
                                mode="json",
                                exclude_none=True,
                            )
                        )
                    )
        finally:
            if self._task_group is not None:
                self._task_group.cancel_scope.cancel()
            self._closed.set()


@asynccontextmanager
async def _connect_local_transport(
    connection: Connection,
) -> AsyncIterator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]
]:
    transport = connection["transport"]
    params = {k: v for k, v in connection.items() if k != "transport"}

    if transport == "stdio":
        command = params.get("command")
        args = params.get("args")
        if not isinstance(command, str):
            msg = "'command' parameter is required for stdio connection"
            raise ValueError(msg)
        if not isinstance(args, list):
            msg = "'args' parameter is required for stdio connection"
            raise ValueError(msg)
        env = params.get("env")
        resolved_env = (
            {k: _expand_env_vars(v) for k, v in env.items()}
            if isinstance(env, dict)
            else None
        )
        server_params = StdioServerParameters(
            command=command,
            args=args,
            env=resolved_env,
            cwd=params.get("cwd"),
            encoding=params.get("encoding", DEFAULT_ENCODING),
            encoding_error_handler=params.get(
                "encoding_error_handler", DEFAULT_ENCODING_ERROR_HANDLER
            ),
        )
        async with stdio_client(server_params) as streams:
            yield streams
        return

    if transport == "sse":
        url = params.get("url")
        if not isinstance(url, str):
            msg = "'url' parameter is required for SSE connection"
            raise ValueError(msg)
        kwargs: dict[str, Any] = {}
        httpx_client_factory = params.get("httpx_client_factory")
        if httpx_client_factory is not None:
            kwargs["httpx_client_factory"] = httpx_client_factory
        async with sse_client(
            url,
            params.get("headers"),
            params.get("timeout", DEFAULT_HTTP_TIMEOUT),
            params.get("sse_read_timeout", DEFAULT_SSE_READ_TIMEOUT),
            auth=params.get("auth"),
            **kwargs,
        ) as streams:
            yield streams
        return

    if transport in {"streamable_http", "streamable-http", "http"}:
        url = params.get("url")
        if not isinstance(url, str):
            msg = "'url' parameter is required for Streamable HTTP connection"
            raise ValueError(msg)
        timeout = _seconds(params.get("timeout", DEFAULT_STREAMABLE_HTTP_TIMEOUT))
        sse_read_timeout = _seconds(
            params.get(
                "sse_read_timeout",
                DEFAULT_STREAMABLE_HTTP_SSE_READ_TIMEOUT,
            )
        )
        client_factory = params.get("httpx_client_factory") or create_mcp_http_client
        http_client = _create_http_client(
            client_factory,
            headers=params.get("headers"),
            timeout=httpx.Timeout(timeout, read=sse_read_timeout),
            auth=params.get("auth"),
        )
        async with (
            http_client,
            streamable_http_client(
                url,
                http_client=http_client,
                terminate_on_close=params.get("terminate_on_close", True),
            ) as (read_stream, write_stream, _),
        ):
            yield read_stream, write_stream
        return

    if transport == "websocket":
        url = params.get("url")
        if not isinstance(url, str):
            msg = "'url' parameter is required for Websocket connection"
            raise ValueError(msg)
        try:
            from mcp.client.websocket import websocket_client  # noqa: PLC0415
        except ImportError:
            msg = (
                "Could not import websocket_client. To use Websocket connections, "
                "install 'mcp[ws]' or 'websockets'."
            )
            raise ImportError(msg) from None
        async with websocket_client(url) as streams:
            yield streams
        return

    msg = (
        f"Unsupported transport: {transport}. "
        "Must be one of: 'stdio', 'sse', 'websocket', 'http'"
    )
    raise ValueError(msg)


def _seconds(value: float | timedelta) -> float:
    return value.total_seconds() if isinstance(value, timedelta) else value


def _create_http_client(
    client_factory: McpHttpClientFactory,
    *,
    headers: dict[str, Any] | None,
    timeout: httpx.Timeout,
    auth: httpx.Auth | None,
) -> httpx.AsyncClient:
    return client_factory(headers=headers, timeout=timeout, auth=auth)


async def run_reverse_websocket_relay(
    *,
    url: str,
    connections: Mapping[str, Connection],
    client_id: str,
    token: str | None = None,
    headers: Mapping[str, str] | None = None,
    reconnect: bool = True,
    reconnect_interval: float = DEFAULT_RECONNECT_INTERVAL,
) -> None:
    """Run a local reverse WebSocket MCP bridge.

    The local process connects outbound to an agent host. Once connected, the
    WebSocket carries standard MCP JSON-RPC messages directly, with no relay
    envelope. Because the stream is naked MCP, exactly one local MCP server can
    be exposed per WebSocket connection.
    """
    while True:
        try:
            await connect_reverse_websocket_relay(
                url=url,
                connections=connections,
                client_id=client_id,
                token=token,
                headers=headers,
            )
        except asyncio.CancelledError:  # noqa: PERF203
            raise
        except Exception:
            if not reconnect:
                raise
            logger.exception("Reverse WebSocket relay disconnected; reconnecting")
            await anyio.sleep(reconnect_interval)
        else:
            if not reconnect:
                return


async def connect_reverse_websocket_relay(
    *,
    url: str,
    connections: Mapping[str, Connection],
    client_id: str,
    token: str | None = None,
    headers: Mapping[str, str] | None = None,
) -> None:
    """Connect once to a reverse WebSocket MCP host and bridge until closed."""
    try:
        from websockets.asyncio.client import connect as ws_connect  # noqa: PLC0415
    except ImportError:
        msg = (
            "Could not import websockets. To use reverse WebSocket relay, "
            "install the optional dependency: 'pip install websockets'."
        )
        raise ImportError(msg) from None

    additional_headers = dict(headers or {})
    if token is not None:
        additional_headers["Authorization"] = f"Bearer {token}"

    connection = _select_single_connection(connections)

    async with ws_connect(
        url,
        additional_headers=additional_headers or None,
        subprotocols=[cast("Subprotocol", "mcp")],
    ) as websocket:
        logger.debug("Reverse WebSocket MCP bridge connected for client %s", client_id)
        async with _DirectMcpBridge(
            connection=connection,
            websocket=websocket,
        ) as bridge:
            await bridge.wait_closed()


def _select_single_connection(connections: Mapping[str, Connection]) -> Connection:
    if len(connections) != 1:
        msg = (
            "Direct reverse WebSocket MCP carries one naked MCP stream per "
            "WebSocket connection; pass exactly one local connection."
        )
        raise ReverseWebSocketRelayError(msg)
    return next(iter(connections.values()))


def _decode_json_rpc(raw_message: str | bytes) -> dict[str, Any]:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode()
    payload = json.loads(raw_message)
    if not isinstance(payload, dict):
        msg = "MCP JSON-RPC message must be a JSON object"
        raise ReverseWebSocketRelayError(msg)
    return payload


__all__ = [
    "ReverseWebSocketRelayError",
    "connect_reverse_websocket_relay",
    "run_reverse_websocket_relay",
]
