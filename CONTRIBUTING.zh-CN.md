# 贡献 OpenLinker Core

English documentation: [CONTRIBUTING.md](./CONTRIBUTING.md)

感谢你改进 OpenLinker Core。本仓库是开源后端边界，负责 Agent 注册中心、运行时网关、
A2A/MCP 协议面和自托管 Core API。

## 开发环境

```bash
docker compose up -d postgres redis
cp .env.example .env
make migrate-up
make test
```

本地示例只能使用占位密钥。不要提交真实 `.env`、runtime token、OAuth secret、客户
payload 或私有 endpoint。

## 范围边界

Core 必须保持不依赖托管云服务。

可以放在这里：

- 认证、会话、注册中心、run、runtime token、A2A、MCP、任务、工作流、交付、本地管理员 API
- 支撑这些开源能力的迁移和存储
- 描述开源 Core 行为的 SDK 契约更新

不要放在这里：

- 钱包余额、扣费、提现、Stripe、价格和计费
- 托管市场排序、商业 Dashboard、Cloud-only 账户产品
- 私有风控、认证或推荐内部策略

## PR 要求

- 改动保持聚焦，并说明用户可见行为。
- API、runtime、迁移或协议行为变化需要测试。
- 公共行为变化要更新 README、SDK 契约或示例。
- 生成文件必须与源文件保持一致。
- 日志、截图和 fixture 中要删除 token 和私有 URL。

## 检查

```bash
gofmt -w .
go test ./...
go test ./... -race -cover
go vet ./...
```

仓库辅助命令：

```bash
make test
make fmt
make demo-a2a
make runtime-loadtest
```

如果某个检查需要外部服务或凭证，请在 PR 中说明跳过原因。

## 迁移

- forward 和 rollback migration 要一起提交。
- migration 应可重复本地测试。
- 破坏性或耗时迁移要在 PR 描述中说明。

## 安全

不要公开提交漏洞 Issue。请按照 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 处理。

## 许可证

贡献即表示你同意贡献内容使用本仓库的 Apache-2.0 许可证。
