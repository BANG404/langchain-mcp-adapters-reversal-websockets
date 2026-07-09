# 反向 WebSocket MCP 示例

本文档说明如何通过 `wss://` 让云端 Agent 访问运行在本地机器上的 MCP 服务器。

默认英文文档见 [README.md](README.md)。

## 云端通过 `wss://` 访问本地 MCP

反向 MCP 的部署方式是：云端只暴露一个 WebSocket 入口，本地机器主动连到这个入口；云端 Agent 不直接访问本地网络，而是把标准 MCP JSON-RPC 消息直接发到这条已建立的 `wss://` 连接上。

```text
本地 MCP server <--stdio/http/ws--> 本地 provider --wss--> 云端 Agent Host <--ClientSession--> 云端 Agent
```

这条 WebSocket 上没有外层 envelope：没有 `hello`，没有 `mcp_message`，没有 `session_id`，也没有嵌套的 `payload`。每条 WebSocket 连接承载一个裸 MCP 会话。

## 1. 在云服务器上准备 Agent Host

先用 demo Agent Host 验证链路：

```bash
uv run --group test python examples/reverse_websocket_relay/cloud_gateway_demo.py \
  --host 0.0.0.0 \
  --port 8765
```

生产环境中应把 `cloud_gateway_demo.py` 替换成你自己的云端 Agent Host 服务。这个服务需要做三件事：

- 接受本地 provider 发起的 WebSocket 连接，子协议为 `mcp`。
- 校验 `Authorization: Bearer <token>` 或你自己的设备认证。
- 把 WebSocket 的裸 JSON-RPC 帧桥接成 MCP SDK 的 `ClientSession`，然后执行 `initialize`、`tools/list`、`tools/call` 等标准 MCP 请求。

## 2. 用 TLS / 反向代理提供 `wss://` 地址

demo Agent Host 本身监听的是普通 `ws://`。公网使用时，让 Nginx、Caddy、负载均衡器或云厂商网关在外层终止 TLS，然后把流量反代到内部服务。

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

配置完成后，本地 provider 连接的地址就是：

```text
wss://mcp.example.com/mcp-tunnel
```

## 3. 在本地机器启动 provider

本地 provider 负责连接本地 MCP server，并主动连到云端 `wss://` Agent Host：

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
        }
    },
)
```

直连裸 MCP 模式下，每条 WebSocket 只能暴露一个本地 MCP server；如果要暴露多个本地 server，请为每个 server 建立一条独立 WebSocket 连接。

## 4. 在云端 Agent 中使用本地 MCP

云端 Agent Host 收到本地 provider 的连接后，直接在 WebSocket 上发送标准 MCP JSON-RPC。例如初始化请求就是裸 JSON-RPC：

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2025-06-18",
    "capabilities": {},
    "clientInfo": {
      "name": "agent-host",
      "version": "1.0.0"
    }
  }
}
```

在云端代码里，`cloud_gateway_demo.py` 展示了最小实现方式：用两个 `anyio` memory stream 把 WebSocket 裸 JSON-RPC 桥接成 MCP SDK 的 `ClientSession`。接上 LangChain 时，可以继续使用普通 MCP session 的加载方式：

```python
from langchain.agents import create_agent
from langchain_mcp_adapters.tools import load_mcp_tools

# session 是 Agent Host 为这条 WebSocket 桥接出的 MCP ClientSession。
await session.initialize()
tools = await load_mcp_tools(session)
agent = create_agent("openai:gpt-4.1", tools)
response = await agent.ainvoke({"messages": "列出我的本地项目目录"})
```

## 5. 关闭会话

当云端 Agent 不再需要这个本地 MCP 会话时，关闭 WebSocket 即可；本地 provider 会关闭对应的本地 MCP transport。`run_reverse_websocket_relay()` 默认会在 WebSocket 断开后重连，适合常驻在用户本地机器上。
