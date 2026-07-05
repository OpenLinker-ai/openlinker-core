# 安全策略

English documentation: [SECURITY.md](./SECURITY.md)

不要用公开 Issue 报告安全漏洞。

优先使用 GitHub 私密漏洞报告。如果不可用，请通过 OpenLinker 公布的安全或支持渠道联系
维护者。报告中请包含受影响仓库、commit 或 release、复现步骤、影响范围，以及是否涉及
真实 token、公开 endpoint 或客户数据。

## 支持版本

OpenLinker Core 目前是 pre-1.0。安全修复面向当前 `main` 分支，以及可用时的最新
release tag。除非维护者明确公告，否则旧 commit 不承诺 backport。

## 敏感区域

- JWT / session 认证
- 用户 access token 和 Agent runtime token
- Agent 注册和 runtime assignment
- A2A/MCP 请求处理
- webhook 和 delivery 签名
- endpoint URL 校验和本地 HTTP 策略
- admin API 和自托管运维界面
- 影响授权的迁移或存储变化

## 报告建议

请提供：

- 受影响 commit、tag 或部署版本
- 最小复现或影响证明
- 期望行为和实际行为
- 是否需要认证才能利用
- 是否有真实 secret 暴露

不要在公开报告、测试、截图或日志里放真实第三方 secret。如果 token 已暴露，请先轮换再
分享细节。

## 披露

维护者会尽快 triage。请在修复、缓解方案或协调披露时间线确定前避免公开披露。
