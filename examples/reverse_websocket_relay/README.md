# Reverse WebSocket MCP Relay Example

This example shows how to expose MCP servers running on a user's local machine to
a cloud agent through an outbound WebSocket connection.

The key difference from the normal `websocket` transport is direction:

```text
normal websocket transport:
cloud/client -> local-or-public MCP server

reverse websocket relay:
local relay -> cloud gateway -> cloud agent
```

Use this when the cloud cannot directly reach the user's local network, but the
local machine can open an outbound `wss://` connection to your cloud.

## Files

- `math_server.py`: a small local stdio MCP server with `add` and `multiply`
  tools.
- `local_relay.py`: the local process that connects to the cloud WebSocket
  gateway and exposes local MCP servers.
- `cloud_gateway_demo.py`: a minimal demo gateway. It accepts the reverse
  WebSocket connection, creates an MCP `ClientSession`, lists tools, calls
  `add`, and exits.

`cloud_gateway_demo.py` is only a runnable demonstration. In production, replace
it with your cloud gateway service and route agent requests through the same
envelope protocol.

## Install Dependencies

From the repository root:

```bash
uv run --group test python -c "import websockets, mcp"
```

For an installed application, make sure `websockets` is available:

```bash
pip install langchain-mcp-adapters websockets
```

## Run The Demo

Terminal 1, start the cloud gateway demo:

```bash
uv run --group test python examples/reverse_websocket_relay/cloud_gateway_demo.py
```

Terminal 2, start the local relay:

```bash
uv run --group test python examples/reverse_websocket_relay/local_relay.py \
  --url ws://127.0.0.1:8765 \
  --client-id local-demo
```

Expected output in Terminal 1:

```text
Cloud gateway listening on ws://127.0.0.1:8765
Relay connected: client_id=local-demo servers=['math']
Tools exposed through relay: ['add', 'multiply']
add(2, 3) returned: 5
```

## Local Relay Usage

The local relay exposes one or more local MCP servers:

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
        },
        "filesystem": {
            "transport": "stdio",
            "command": "npx",
            "args": [
                "-y",
                "@modelcontextprotocol/server-filesystem",
                "/Users/me/work",
            ],
        },
    },
)
```

Supported local MCP transports are the same transport names used by the adapter:

- `stdio`
- `sse`
- `http` / `streamable_http`
- `websocket`

## Cloud Gateway Protocol

After connecting, the local relay sends:

```json
{
  "type": "hello",
  "protocol_version": 1,
  "client_id": "local-demo",
  "servers": ["math"]
}
```

The cloud gateway sends MCP JSON-RPC messages through an envelope:

```json
{
  "type": "mcp_message",
  "session_id": "session-1",
  "server": "math",
  "payload": {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }
}
```

The local relay returns the MCP response in the same envelope shape:

```json
{
  "type": "mcp_message",
  "session_id": "session-1",
  "server": "math",
  "payload": {
    "jsonrpc": "2.0",
    "id": 1,
    "result": {}
  }
}
```

When the cloud no longer needs a relayed MCP session, it should send:

```json
{
  "type": "session_closed",
  "session_id": "session-1",
  "server": "math"
}
```

## Production Notes

- Use `wss://`, not plaintext `ws://`.
- Authenticate the relay with a short-lived token or device authorization flow.
- Keep a per-user allowlist of exposed local MCP servers.
- Consider tool-level approval for sensitive local actions.
- Do not log tool arguments that may contain local file paths or secrets.
- Route every cloud request by authenticated `user_id`, `client_id`, `server`,
  and `session_id`.
