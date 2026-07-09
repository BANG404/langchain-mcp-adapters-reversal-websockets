"""Reverse WebSocket relay for exposing local MCP servers to cloud agents."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, Self

import anyio
import httpx
from mcp import StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import create_mcp_http_client, streamable_http_client
from mcp.shared.message import SessionMessage
from mcp.types import JSONRPCMessage

from langchain_mcp_adapters.relay_protocol import REVERSE_WS_PROTOCOL_VERSION
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

logger = logging.getLogger(__name__)

DEFAULT_RECONNECT_INTERVAL = 5.0


class _WebSocket(Protocol):
    async def send(self, message: str) -> None: ...


class ReverseWebSocketRelayError(Exception):
    """Error raised by the reverse WebSocket relay."""


class _RelayedSession:
    def __init__(
        self,
        *,
        session_id: str,
        server_name: str,
        connection: Connection,
        websocket: _WebSocket,
        send_lock: anyio.Lock,
    ) -> None:
        self.session_id = session_id
        self.server_name = server_name
        self.connection = connection
        self.websocket = websocket
        self.send_lock = send_lock
        self._exit_stack = AsyncExitStack()
        self._write_stream: MemoryObjectSendStream[SessionMessage] | None = None
        self._task_group: anyio.abc.TaskGroup | None = None

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

    async def send_to_local(self, payload: dict[str, Any]) -> None:
        if self._write_stream is None:
            msg = "Relayed session is not open"
            raise ReverseWebSocketRelayError(msg)
        message = JSONRPCMessage.model_validate(payload)
        await self._write_stream.send(SessionMessage(message=message))

    async def _forward_local_messages(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
    ) -> None:
        async with read_stream:
            async for item in read_stream:
                if isinstance(item, Exception):
                    await _send_envelope(
                        self.websocket,
                        self.send_lock,
                        {
                            "type": "error",
                            "session_id": self.session_id,
                            "server": self.server_name,
                            "message": str(item),
                        },
                    )
                    continue

                await _send_envelope(
                    self.websocket,
                    self.send_lock,
                    {
                        "type": "mcp_message",
                        "session_id": self.session_id,
                        "server": self.server_name,
                        "payload": item.message.model_dump(
                            by_alias=True, mode="json", exclude_none=True
                        ),
                    },
                )


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
    """Run a local relay that exposes MCP servers through an outbound WebSocket.

    The relay connects to a cloud gateway, sends a `hello` envelope listing the
    available local servers, and then forwards MCP JSON-RPC messages between the
    gateway and local MCP transports. Each `(server, session_id)` pair receives
    its own local MCP transport connection.
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
    """Connect once to a reverse WebSocket gateway and relay until disconnected."""
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

    async with ws_connect(
        url,
        additional_headers=additional_headers or None,
        subprotocols=["mcp-reverse"],
    ) as websocket:
        send_lock = anyio.Lock()
        sessions: dict[tuple[str, str], _RelayedSession] = {}

        await _send_envelope(
            websocket,
            send_lock,
            {
                "type": "hello",
                "protocol_version": REVERSE_WS_PROTOCOL_VERSION,
                "client_id": client_id,
                "servers": sorted(connections.keys()),
            },
        )

        async with AsyncExitStack() as exit_stack:
            async for raw_message in websocket:
                envelope = _decode_envelope(raw_message)
                message_type = envelope.get("type")

                if message_type == "mcp_message":
                    await _handle_cloud_mcp_message(
                        envelope=envelope,
                        connections=connections,
                        sessions=sessions,
                        websocket=websocket,
                        send_lock=send_lock,
                        exit_stack=exit_stack,
                    )
                elif message_type == "session_closed":
                    await _close_relayed_session(envelope, sessions)
                else:
                    logger.debug("Ignoring relay envelope with type %r", message_type)


async def _handle_cloud_mcp_message(
    *,
    envelope: dict[str, Any],
    connections: Mapping[str, Connection],
    sessions: dict[tuple[str, str], _RelayedSession],
    websocket: _WebSocket,
    send_lock: anyio.Lock,
    exit_stack: AsyncExitStack,
) -> None:
    server_name = envelope.get("server")
    session_id = envelope.get("session_id")
    payload = envelope.get("payload")

    if not isinstance(server_name, str) or not isinstance(session_id, str):
        await _send_error(websocket, send_lock, "Missing server or session_id")
        return
    if not isinstance(payload, dict):
        await _send_error(
            websocket,
            send_lock,
            "Missing MCP payload",
            server=server_name,
            session_id=session_id,
        )
        return
    if server_name not in connections:
        await _send_error(
            websocket,
            send_lock,
            f"Unknown local MCP server: {server_name}",
            server=server_name,
            session_id=session_id,
        )
        return

    key = (server_name, session_id)
    session = sessions.get(key)
    if session is None:
        session = _RelayedSession(
            session_id=session_id,
            server_name=server_name,
            connection=connections[server_name],
            websocket=websocket,
            send_lock=send_lock,
        )
        await exit_stack.enter_async_context(session)
        sessions[key] = session

    await session.send_to_local(payload)


async def _close_relayed_session(
    envelope: dict[str, Any],
    sessions: dict[tuple[str, str], _RelayedSession],
) -> None:
    session_id = envelope.get("session_id")
    server_name = envelope.get("server")
    if not isinstance(session_id, str):
        return

    keys = [
        key
        for key in sessions
        if key[1] == session_id and (server_name is None or key[0] == server_name)
    ]
    for key in keys:
        session = sessions.pop(key)
        await session.__aexit__(None, None, None)


def _decode_envelope(raw_message: str | bytes) -> dict[str, Any]:
    if isinstance(raw_message, bytes):
        raw_message = raw_message.decode()
    envelope = json.loads(raw_message)
    if not isinstance(envelope, dict):
        msg = "Relay envelope must be a JSON object"
        raise ReverseWebSocketRelayError(msg)
    return envelope


async def _send_error(
    websocket: _WebSocket,
    send_lock: anyio.Lock,
    message: str,
    *,
    server: str | None = None,
    session_id: str | None = None,
) -> None:
    envelope: dict[str, Any] = {"type": "error", "message": message}
    if server is not None:
        envelope["server"] = server
    if session_id is not None:
        envelope["session_id"] = session_id
    await _send_envelope(websocket, send_lock, envelope)


async def _send_envelope(
    websocket: _WebSocket,
    send_lock: anyio.Lock,
    envelope: dict[str, Any],
) -> None:
    async with send_lock:
        await websocket.send(json.dumps(envelope))


__all__ = [
    "ReverseWebSocketRelayError",
    "connect_reverse_websocket_relay",
    "run_reverse_websocket_relay",
]
