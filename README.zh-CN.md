# OpenLinker Core

OpenLinker Core 是用于注册、发现和运行 Agent 的开源控制面。一次自托管部署即可统一处理
REST、SDK、MCP 和 A2A 调用，并把请求路由到公网 Endpoint、远程 MCP Server，或从本地、
内网主动接入的 Agent。Core 带有独立 Web UI，数据和部署策略均由部署方管理。

English documentation: [README.md](./README.md)

## 状态

OpenLinker Core 目前仍是 pre-1.0。运行时模型已经可用，但 API 细节、SDK 契约、
数据库迁移和部署默认值仍可能调整。生产或长期部署请固定 commit / tag，并在升级前阅读
[CHANGELOG.md](./CHANGELOG.md)。

具备细粒度 permission grant 的 User Token 属于开源 Core 的正式产品契约，用于用户侧 REST、SDK、MCP 和
A2A 调用。Core 已在本地签发和验证 `ol_user_*`，保存可限定资源的 Core 权限，并通过
JWT-only 的 `/api/v1/user-tokens` 管理。Hosted 服务可通过受内部凭据保护的内省接口验证
同一枚 Token，再叠加自己的增量权限。

## 范围

包含：

- 用户认证和 JWT 会话
- 用于用户侧 API 与协议调用的 User Token 细粒度权限
- Agent 注册、可见性、分类、技能和 benchmark
- 用于自注册和运行接入的 Agent Token
- run 创建、状态、事件流、artifact 和消息
- `direct_http`、`mcp_server`、`runtime` 三种调用模式
- A2A JSON-RPC / HTTP+JSON、Agent Card 和可选 gRPC
- MCP HTTP 入口和 REST fallback API
- 任务、工作流、交付、webhook 和本地管理员 API
- 基于 Postgres / Redis 的自托管部署

托管产品边界：

- 钱包余额、扣费、提现和 Stripe 流程
- 托管市场排序、商业 Dashboard 组合
- 托管账号、令牌策略和商业访问 Dashboard
- 官方认证、推荐和风控内部策略

这些服务保留在托管产品层，不会成为 Core 的运行依赖。

## 开源架构图

开源仓库以 Core 作为共同的注册表和运行控制面。托管部署可以在 Core API 边界接入
可选 bridge，但闭源产品模块不属于本图，也不应成为本仓库依赖。

```mermaid
flowchart LR
  CoreWeb["openlinker-core-web<br/>自托管 UI"] -->|"REST / session API"| Core
  SDKs["openlinker-go / openlinker-js / openlinker-python<br/>客户端与 runtime SDK"] -->|"HTTP / A2A / MCP binding"| Core
  MCPCaller["MCP 或 A2A 调用方"] -->|"tool call / message/send"| Core

  HostedBridge["Hosted Bridge<br/>可选部署适配层"] -.->|"授权后的 Core API"| Core

  Core["openlinker-core<br/>auth / registry / runs / events"]

  Core -->|"direct_http"| HTTPAgent["公网 HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["远程 MCP / JSON-RPC server"]
  Core -->|"runtime<br/>WebSocket 优先，长轮询兜底"| RuntimeWorker["SDK Runtime Worker"]
  RuntimeWorker -->|"类型化 RuntimeContext"| Handler["应用处理器"]

  Core -.->|"runtime<br/>可选兼容路径"| AdapterWorker["Go SDK Runtime Worker"]
  AdapterWorker --> AgentNode["Agent Node Adapter"]
  AgentNode -->|"http / command / a2a / codex"| Backend["现有 Agent 后端"]
```

## 快速开始

依赖：

- Go 1.25 或更高版本
- Docker，或本地 Postgres / Redis
- `make`

启动依赖：

```bash
docker compose up -d postgres redis
```

创建本地配置：

```bash
cp .env.example .env
```

至少设置这些值：

```bash
DATABASE_URL=postgres://dev:dev@127.0.0.1:5432/openlinker?sslmode=disable
JWT_SECRET=replace-with-32-byte-random-secret
FRONTEND_URL=http://localhost:3000
ALLOW_LOCAL_HTTP_ENDPOINTS=true
```

生成开发密钥：

```bash
openssl rand -hex 32
```

应用迁移并启动 API：

```bash
make migrate-up
make run
```

默认 API 地址是 `http://localhost:8080`。

健康检查：

```bash
curl http://localhost:8080/healthz
curl --fail http://localhost:8080/readyz
```

`/healthz` 只判断进程是否存活。`/readyz` 还会核对数据库中的集群模式、预期副本、
release、schema、Runtime 契约，以及 HA 模式下的 Redis signal 依赖。Redis 故障会让
HA 实例退出就绪状态，但不会停止 PostgreSQL reconcile。

## 初始管理员 Bootstrap

应用 migration 后，Core 会在正常 API 启动时检查是否已经存在 active admin。在
`local`、`dev`、`development` 或 `test` 环境中，可以自动创建本地初始化管理员：

- 邮箱：`admin@openlinker.ai`
- 显示名：`OpenLinker Admin`
- 仅限本地环境的密码：`openlinker-admin`

其他所有 `ENV`（包括 staging 和 production）必须显式设置
`OPENLINKER_BOOTSTRAP_ADMIN_EMAIL` 和 `OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD`，
Core 会先校验这些值，再检查是否已有 active admin。密码长度必须为 12–72 bytes，且不能
等于本地默认密码；邮箱不得使用 `.local` 域名。缺少或使用不安全配置时 Core 会直接拒绝
启动。数据库里已有 active admin 时，会在校验通过后跳过 bootstrap，不会重置密码。

手动修复命令仍然保留：

```bash
make bootstrap-admin
```

该命令同时支持上述环境变量与 `-env`、`-email`、`-password` 参数，并且是幂等的：
如果配置邮箱已经存在，会把该用户提升为 admin 并更新密码。

生产环境不要开启 `ALLOW_LOCAL_HTTP_ENDPOINTS`，也不要使用示例密钥。

## 常用配置

常见必填项：

- `DATABASE_URL`
- `JWT_SECRET`
- `FRONTEND_URL`

常见可选项：

- `REDIS_URL`
- `RUNTIME_HA_MODE` — `expected_replicas` 大于 1 时必须设为 `true`
- `OPENLINKER_RELEASE_ID` / `OPENLINKER_GIT_SHA` — 镜像构建时注入；生产环境拒绝占位值
- `API_URL`
- `OAUTH_CALLBACK_BASE_URL`、`OAUTH_ALLOWED_FRONTEND_ORIGINS`
- `OAUTH_SESSION_SECRET`
- `GOOGLE_OAUTH_CLIENT_ID` / `GITHUB_OAUTH_CLIENT_ID`（OAuth 登录）
- `ALLOW_LOCAL_HTTP_ENDPOINTS` — 本地开发请设为 `true`
- `RUNTIME_ENDPOINT_RUN_*` — run 超时 worker 参数

### LLM 配置（可选，用于任务路由和 benchmark）

未配置 LLM 时，任务路由自动降级到关键词匹配。如需开启 LLM 辅助路由和 skill benchmark：

```bash
# 方案 A：任意 OpenAI 兼容 API（自托管、Ollama、Azure 等）
LLM_OPENAI_URL=https://api.openai.com/v1
LLM_OPENAI_API_KEY=sk-...
LLM_OPENAI_MODEL=gpt-4o-mini       # 可选，默认 gpt-4o-mini

# 方案 B：内部代理（仅限 openlinker.ai 云端部署）
LLM_COMPLETE_URL=http://internal-llm-proxy/complete
```

`LLM_COMPLETE_URL` 为空时，方案 A 生效。方案 B 仅适用于 openlinker.ai 私有云部署。

### User Token 内省与私有服务环境变量

User Token 的签发和验证已经是 Core 本地能力，不需要外部验证器。Hosted 服务若要叠加自己的
增量权限，可通过 Core 内省同一枚 Token。

| 变量 | 用途 | 自托管 |
|------|------|------|
| `OPENLINKER_INTERNAL_TOKEN` | 保护 `POST /internal/user-tokens/introspect`，也可供 LLM 代理等受信私有服务鉴权 | 未启用内部服务集成时留空 |

## 常用命令

```bash
make help              # 列出 Makefile target
make deps              # 下载并整理 Go 依赖
make build             # 构建 bin/api
make run               # 使用 .env 构建并运行
make test              # go test ./... -race -cover
make fmt               # gofmt 和 go vet
make migrate-up        # 应用迁移
make migrate-down      # 回退一个迁移
make runtime-loadtest  # 分别通过 WebSocket 与长轮询压测 Runtime Worker
```

## Runtime 模式

Core 每五秒使用 PostgreSQL 时间刷新集群成员记录。多副本部署必须设置
`RUNTIME_HA_MODE=true`；所有 live Core 的 release、schema checksum 和 OpenLinker Runtime
契约完全一致后，`/readyz` 才会成功。migration 默认进入 `hard_maintenance`，Core
不会把尚未完成切换的数据库误判为可服务状态。

破坏性 Runtime migration 使用镜像内置的 `runtime-cutover`。`status` 与
`preflight` 只输出脱敏 JSON 证据；`drain`、`hard-maintenance`、`reopen` 都必须显式
传入 cluster control 的 CAS version，`reopen` 还必须匹配当前 cutover ID。只有数据库
契约、精确 live 副本数、release、schema checksum 与 Redis HA 全部一致时才能 reopen。
管理端通过 `GET /api/v1/admin/runtime/maintenance` 只读同一份状态，不提供模式写操作。

```bash
./runtime-cutover preflight --require-exclusive --require-no-members
./runtime-cutover status
./runtime-cutover reopen --expected-version=<version> --cutover-id=<uuid>
```

对每个 Agent 使用最简单可达的模式：

1. `direct_http`：Core 调用稳定的 HTTPS Agent endpoint。
2. `mcp_server`：Core 调用已有远程 HTTP JSON-RPC 或 MCP endpoint。
3. `runtime`：Runtime Worker 接收分配的运行。默认使用 `auto` 传输策略：优先建立
   出站 WebSocket，网络无法稳定维持连接时切到长轮询。两种传输共用同一套
   Session、lease、ACK、resume、fence 与本地 spool，不是两个市场接入模式。

正常接入只需给 Runtime Worker 配置公开的 OpenLinker 地址 `OPENLINKER_URL`。SDK 会在
不携带 Agent Token 和客户端证书的情况下读取 `/.well-known/openlinker.json`，再从
`base_urls.runtime` 取得专用 mTLS 连接 origin。`RUNTIME_MTLS_API_URL` 只由部署者配置，
不是创作者需要再填写的第二个地址。

每个已分配或已 claim 的 run 必须最终提交一次终态结果。

### Runtime Node 证书签发

Reliable OpenLinker Runtime 会同时校验 Runtime Worker 的客户端证书和数据库里的
`runtime_nodes` 记录。客户端 CA 私钥只保存在运维侧的签发主机上，不得复制进 Core
容器、写入 `.env`，也不得和服务端 TLS 私钥放在同一个挂载目录。Core 运行时只需要
通过 `RUNTIME_MTLS_CLIENT_CA_FILE` 读取 CA 证书。

应用当前 migration 并构建 Core 后，在能够临时访问 Postgres 的安全签发主机执行：

```bash
make build
DATABASE_URL='postgres://...' ./bin/api runtime-node issue \
  --ca-cert /secure/runtime-client-ca.crt \
  --ca-key /secure/runtime-client-ca.key \
  --display-name 'Singapore worker 01' \
  --capacity 4 \
  --cert-out ./node-pki/runtime-node.crt \
  --key-out ./node-pki/runtime-node.key
```

Unix 上的 CA 私钥文件权限必须仅限 owner（`0600` 或 `0400`）。输出目录必须事先存在。
该命令会生成 ECDSA P-256 私钥和仅限 `clientAuth` 的证书，
再把随机 serial、SPKI SHA-256 指纹和当前 OpenLinker Runtime 契约一次性登记到数据库，成功后
输出 JSON 审计记录。命令不会覆盖任何已有文件；私钥权限固定为 `0600`，证书为
`0644`。`--node-id` 可省略并自动生成。`--node-version` 默认为
`openlinker-go/runtime-worker`。Agent Node 等 Adapter 如果声明了另一种实现，签发时必须
传入该实现的精确版本标识。

交付前可离线检查证书、私钥和 CA 是否匹配：

```bash
./bin/api runtime-node inspect \
  --cert ./node-pki/runtime-node.crt \
  --key ./node-pki/runtime-node.key \
  --ca-cert /secure/runtime-client-ca.crt
```

把 JSON 中的 `node_id`、登记 capacity、交付的证书和私钥，以及 Runtime 服务端
CA 传入 SDK `RuntimeWorker` 配置。可选的 Agent Node Adapter 通过
`OPENLINKER_NODE_ID`、`OPENLINKER_AGENT_NODE_CAPACITY`、
`OPENLINKER_AGENT_NODE_MTLS_CERT_FILE`、`OPENLINKER_AGENT_NODE_MTLS_KEY_FILE` 和
`OPENLINKER_AGENT_NODE_MTLS_CA_FILE` 接收同一组值。客户端 CA 证书只分发给 Core，
其私钥始终留在所有 OpenLinker 运行服务之外。

## 安全

- 不要记录或暴露明文 Agent Token。
- 不要把 Agent Token 传给后端子进程。
- 生产环境保持 `ALLOW_LOCAL_HTTP_ENDPOINTS=false`。
- 公开 `direct_http` 和 `mcp_server` endpoint 必须使用 HTTPS。
- 如果 token 被打印、提交或发给了错误边界，请立即轮换。

安全漏洞请通过 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 报告，不要发公开 Issue。

## 贡献

提交 PR 前请阅读 [CONTRIBUTING.zh-CN.md](./CONTRIBUTING.zh-CN.md)。Core 必须保持
独立于商业 Cloud 模块；公共行为变化需要同步 SDK 契约或测试。

## 支持和发布

- 支持说明：[SUPPORT.zh-CN.md](./SUPPORT.zh-CN.md)
- 发布清单：[RELEASE.zh-CN.md](./RELEASE.zh-CN.md)
- 英文变更记录：[CHANGELOG.md](./CHANGELOG.md)
- 行为准则：[CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)

## 许可证

Apache-2.0。详见 [LICENSE](./LICENSE)。
