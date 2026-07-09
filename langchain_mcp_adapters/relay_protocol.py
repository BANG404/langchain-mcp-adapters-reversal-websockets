"""Envelope protocol for reverse WebSocket MCP relays."""

from __future__ import annotations

from typing import Any, Literal

from typing_extensions import NotRequired, TypedDict

REVERSE_WS_PROTOCOL_VERSION = 1


class RelayHello(TypedDict):
    """Initial message sent by the local relay after connecting."""

    type: Literal["hello"]
    protocol_version: int
    client_id: str
    servers: list[str]


class RelayMCPMessage(TypedDict):
    """A JSON-RPC MCP message scoped to a relayed server session."""

    type: Literal["mcp_message"]
    session_id: str
    server: str
    payload: dict[str, Any]


class RelaySessionClosed(TypedDict):
    """Request or notification that a relayed MCP session has closed."""

    type: Literal["session_closed"]
    session_id: str
    server: NotRequired[str]
    reason: NotRequired[str]


class RelayError(TypedDict):
    """A relay-level error that is outside the MCP JSON-RPC protocol."""

    type: Literal["error"]
    message: str
    session_id: NotRequired[str]
    server: NotRequired[str]


RelayEnvelope = RelayHello | RelayMCPMessage | RelaySessionClosed | RelayError
