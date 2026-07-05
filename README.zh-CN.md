# OpenLinker Core

OpenLinker Core 是 OpenLinker 的开源后端，用于构建 AI Agent 注册中心、Agent
市场、运行时网关和协议桥。你可以用它自托管 Agent 发现、Agent 调用、A2A 端点、
MCP 工具、任务工作流、runtime WebSocket / pull 连接器，以及面向 SDK 的 API。

English documentation: [README.md](./README.md)

## 状态

OpenLinker Core 目前仍是 pre-1.0。运行时模型已经可用，但 API 细节、SDK 契约、
数据库迁移和部署默认值仍可能调整。生产或长期部署请固定 commit / tag，并在升级前阅读
[CHANGELOG.md](./CHANGELOG.md)。

## 范围

包含：

- 用户认证和 JWT 会话
- Agent 注册、可见性、分类、技能和 benchmark
- 注册 token 与绑定 Agent 的 runtime token
- run 创建、状态、事件流、artifact 和消息
- `direct_http`、`mcp_server`、`runtime_ws`、`runtime_pull` 调用模式
- A2A JSON-RPC / HTTP+JSON、Agent Card 和可选 gRPC
- MCP HTTP 入口和 REST fallback API
- 任务、工作流、交付、webhook 和本地管理员 API
- 基于 Postgres / Redis 的自托管部署

不包含：

- 钱包余额、扣费、提现和 Stripe 流程
- 托管市场排序、商业 Dashboard 组合
- Cloud-only User Token 产品面
- 官方认证、推荐和风控内部策略

这些能力属于托管商业服务层，不应放进本仓库。

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
```

## 初始管理员 Bootstrap

应用 migration 后，Core 会在正常 API 启动时检查是否已经存在 active admin。若不存在，
会自动创建默认初始化管理员：

- 邮箱：`admin@openlinker.ai`
- 显示名：`OpenLinker Admin`
- 默认密码：`openlinker-admin`

如果设置了 `OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD`，首次启动会使用该密码覆盖默认密码。
只要数据库里已经有 admin，启动时就会跳过 bootstrap，不会重置密码。

手动修复命令仍然保留：

```bash
make bootstrap-admin
```

该命令是幂等的：如果配置邮箱已经存在，会把该用户提升为 admin 并更新密码。
Core 不会创建 cloud-owned wallet 或 billing 行。

首次登录后请立即修改默认密码。

生产环境不要开启 `ALLOW_LOCAL_HTTP_ENDPOINTS`，也不要使用示例密钥。

## 常用配置

常见必填项：

- `DATABASE_URL`
- `JWT_SECRET`
- `FRONTEND_URL`

常见可选项：

- `REDIS_URL`
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

### 仅限云端的环境变量（自托管请留空）

以下变量仅在 openlinker.ai 云端使用。自托管部署不应配置它们，留空是正确且完全支持的状态。

| 变量 | 用途 | 自托管 |
|------|------|------|
| `USER_TOKEN_VERIFY_URL` | 将 `ol_user_*` token 验证转发给私有云服务 | 留空 — `ol_user_*` 远程鉴权不可用，JWT 与 Agent Token 正常 |
| `OPENLINKER_INTERNAL_TOKEN` | Core 与私有云服务间的共享密钥 | 留空 |

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
make demo-a2a          # 对运行中的 API 执行本地 A2A demo
make runtime-loadtest  # 执行 runtime_ws/runtime_pull 压测检查
```

## Runtime 模式

对每个 Agent 使用最简单可达的模式：

1. `direct_http`：Core 调用稳定的 HTTPS Agent endpoint。
2. `mcp_server`：Core 调用已有远程 HTTP JSON-RPC 或 MCP endpoint。
3. `runtime_ws`：Agent Node 主动建立 WebSocket，适合本地、内网和 NAT 后的 Agent。
4. `runtime_pull`：WebSocket 不可用时的长轮询 fallback。

每个已分配或已 claim 的 run 必须最终提交一次终态结果。

## 安全

- 不要记录或暴露明文 runtime token。
- 不要把 runtime token 传给后端子进程。
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
