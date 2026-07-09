# Reverse WebSocket MCP Example

This example shows how to expose MCP servers running on a user's local machine to
a cloud agent through an outbound WebSocket connection.

The key difference from the normal `websocket` transport is direction:

```text
normal websocket transport:
cloud/client -> local-or-public MCP server

reverse websocket MCP:
local provider -> cloud agent host
```

Use this when the cloud cannot directly reach the user's local network, but the
local machine can open an outbound `wss://` connection to your cloud.

The WebSocket carries standard MCP JSON-RPC messages directly. There is no
`hello` message, relay envelope, `session_id`, or nested `payload` object.

For Chinese deployment notes, see
[README.zh-CN.md](README.zh-CN.md).

## Files

- `math_server.py`: a small local stdio MCP server with `add` and `multiply`
  tools.
- `local_relay.py`: the local process that connects to the cloud WebSocket
  host and exposes one local MCP server.
- `cloud_gateway_demo.py`: a minimal demo agent host. It accepts the reverse
  WebSocket connection, creates an MCP `ClientSession`, lists tools, calls
  `add`, and exits.

`cloud_gateway_demo.py` is only a runnable demonstration. In production, replace
it with your agent host service and route agent requests through the same direct
MCP stream.

## Install Dependencies

From the repository root:

```bash
uv run --group test python -c "import websockets, mcp"
```

For an installed application, make sure `websockets` is available:

```bash
pip install langchain-mcp-adapters-reversal-websockets websockets
```

## Run The Demo

Terminal 1, start the cloud agent host demo:

```bash
uv run --group test python examples/reverse_websocket_relay/cloud_gateway_demo.py
```

Terminal 2, start the local provider:

```bash
uv run --group test python examples/reverse_websocket_relay/local_relay.py \
  --url ws://127.0.0.1:8765 \
  --client-id local-demo
```

Expected output in Terminal 1:

```text
Agent host listening on ws://127.0.0.1:8765
Local MCP provider connected
Tools exposed through reverse WebSocket: ['add', 'multiply']
add(2, 3) returned: 5
```

## Local Relay Usage

The local process exposes exactly one local MCP server per WebSocket connection:

```python
from langchain_mcp_adapters.reverse_ws import run_reverse_websocket_relay

await run_reverse_websocket_relay(
    url="wss://cloud.example.com/mcp-tunnel",
    client_id="user-laptop-001",
    token="short-lived-token",
    connections={
        "math": {
            "transport": "stdio",
            "command": "python",
            "args": ["./math_server.py"],
        }
    },
)
```

Supported local MCP transports are the same transport names used by the adapter:

- `stdio`
- `sse`
- `http` / `streamable_http`
- `websocket`

## Agent Host Protocol

After the WebSocket handshake, the agent host acts as the MCP client and starts
the normal MCP lifecycle by sending `initialize` directly on the socket:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {},
    "clientInfo": {"name": "agent-host", "version": "1.0.0"}
  }
}
```

The local provider returns standard MCP JSON-RPC responses directly:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {}
}
```

## Production Notes

- Use `wss://`, not plaintext `ws://`.
- Authenticate the relay with a short-lived token or device authorization flow.
- Keep a per-user allowlist of exposed local MCP servers.
- Consider tool-level approval for sensitive local actions.
- Do not log tool arguments that may contain local file paths or secrets.
- Route each authenticated user or device to its own WebSocket connection. Open
  separate WebSocket connections when you need to expose multiple local MCP
  servers.
