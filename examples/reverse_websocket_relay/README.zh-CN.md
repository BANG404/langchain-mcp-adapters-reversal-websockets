# 反向 WebSocket MCP Relay 示例

本文档说明如何使用反向 MCP，通过 `wss://` 让云服务器访问运行在本地机器上的 MCP 服务器。

默认英文文档见 [README.md](README.md)。

## 云服务器通过 `wss://` 访问本地 MCP

反向 MCP 的部署方式是：云服务器只暴露一个 WebSocket 网关，本地机器主动连到这个网关；云端 Agent 不直接访问本地网络，而是把 MCP JSON-RPC 消息通过这条已建立的 `wss://` 连接转发给本地 MCP 服务器。

```text
本地 MCP server <--stdio/http/ws--> 本地 relay --wss--> 云端 gateway <--ClientSession--> 云端 Agent
```

## 1. 在云服务器上准备 WebSocket 网关

先用 demo 网关验证链路：

```bash
uv run --group test python examples/reverse_websocket_relay/cloud_gateway_demo.py \
  --host 0.0.0.0 \
  --port 8765
```

生产环境中应把 `cloud_gateway_demo.py` 替换成你自己的云端 gateway 服务。这个服务需要做四件事：

- 接受本地 relay 发起的 WebSocket 连接，子协议为 `mcp-reverse`。
- 校验 `Authorization: Bearer <token>` 或你自己的设备认证。
- 保存 `hello` 包里的 `client_id` 和 `servers`，把它们绑定到当前用户或租户。
- 当云端 Agent 要调用本地 MCP 工具时，创建 MCP `ClientSession`，并把 session 的读写流转换成 relay envelope。

## 2. 用 TLS / 反向代理提供 `wss://` 地址

demo 网关本身监听的是普通 `ws://`。公网使用时，让 Nginx、Caddy、负载均衡器或云厂商网关在外层终止 TLS，然后把流量反代到内部服务。

一个 Nginx 示例：

```nginx
server {
    listen 443 ssl;
    server_name mcp.example.com;

    ssl_certificate /etc/letsencrypt/live/mcp.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/mcp.example.com/privkey.pem;

    location /mcp-tunnel {
        proxy_pass http://127.0.0.1:8765;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header Authorization $http_authorization;
        proxy_read_timeout 3600s;
    }
}
```

配置完成后，本地 relay 连接的地址就是：

```text
wss://mcp.example.com/mcp-tunnel
```

## 3. 在本地机器启动 relay

本地 relay 负责连接本地 MCP server，并主动连到云端 `wss://` gateway：

```bash
uv run --group test python examples/reverse_websocket_relay/local_relay.py \
  --url wss://mcp.example.com/mcp-tunnel \
  --client-id user-laptop-001 \
  --token "$MCP_RELAY_TOKEN"
```

也可以在自己的程序中直接调用：

```python
from langchain_mcp_adapters.reverse_ws import run_reverse_websocket_relay

await run_reverse_websocket_relay(
    url="wss://mcp.example.com/mcp-tunnel",
    client_id="user-laptop-001",
    token="short-lived-token",
    connections={
        "filesystem": {
            "transport": "stdio",
            "command": "npx",
            "args": [
                "-y",
                "@modelcontextprotocol/server-filesystem",
                "/Users/me/work",
            ],
        },
        "local-api": {
            "transport": "http",
            "url": "http://127.0.0.1:8000/mcp",
        },
    },
)
```

`connections` 里的 key 是云端看到的 MCP server 名称。上面的例子会向云端声明 `filesystem` 和 `local-api` 两个本地 MCP server。

## 4. 在云端 Agent 中使用 relay 暴露的 MCP

云端 gateway 收到本地 relay 的连接后，需要为每次 Agent 会话选择目标 `client_id`、`server` 和 `session_id`，然后把 MCP 消息包在 envelope 里发送给本地 relay：

```json
{
  "type": "mcp_message",
  "session_id": "agent-run-123",
  "server": "filesystem",
  "payload": {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }
}
```

在云端代码里，`cloud_gateway_demo.py` 展示了最小实现方式：用两个 `anyio` memory stream 把 WebSocket envelope 桥接成 MCP SDK 的 `ClientSession`。接上 LangChain 时，可以继续使用普通 MCP session 的加载方式：

```python
from langchain.agents import create_agent
from langchain_mcp_adapters.tools import load_mcp_tools

# session 是 gateway 为某个 client_id/server/session_id 桥接出的 MCP ClientSession。
await session.initialize()
tools = await load_mcp_tools(session)
agent = create_agent("openai:gpt-4.1", tools)
response = await agent.ainvoke({"messages": "列出我的本地项目目录"})
```

## 5. 关闭会话

当云端 Agent 不再需要某个本地 MCP 会话时，gateway 应发送：

```json
{
  "type": "session_closed",
  "session_id": "agent-run-123",
  "server": "filesystem"
}
```

本地 relay 收到后会关闭对应 `(server, session_id)` 的本地 MCP transport。`run_reverse_websocket_relay()` 默认会在 WebSocket 断开后重连，适合常驻在用户本地机器上。
