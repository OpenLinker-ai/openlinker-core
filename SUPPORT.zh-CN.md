# 支持

English documentation: [SUPPORT.md](./SUPPORT.md)

可用 GitHub Issues 报告可复现 bug、文档问题，以及符合 OpenLinker Core 开源范围的功能
请求。

## 适合提交 Issue 的内容

- API 或 runtime 行为与 README / 测试不一致
- migration、本地启动或 Docker Compose 问题
- A2A、MCP、runtime WebSocket、runtime pull、任务、工作流或交付问题
- 阻碍自托管使用的文档缺口
- 面向开源 Core 能力的聚焦功能请求

## 提交前请确认

- 搜索已有 Issue 和近期 commit。
- 在最新 `main` 或指定 release 上确认问题。
- 提供操作系统、Go 版本、数据库版本和 commit SHA。
- 提供复现步骤、期望行为、实际行为和脱敏日志。
- 删除 JWT、runtime token、OAuth secret、私有 URL、客户数据和本地 `.env` 值。

## 不在这里处理

- 安全漏洞；请看 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md)
- 商业计费、钱包、Stripe、提现或托管 Dashboard 请求
- 无法公开复现的私有部署调试
- 需要访问私有数据、私有日志或真实 secret 的问题

## 跨仓库问题

跨多个 OpenLinker 仓库的问题请包含：

- 受影响组件名称
- commit SHA 或 package version
- 哪个组件收到请求
- 哪个组件返回异常响应
