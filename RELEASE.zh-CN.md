# 发布流程

English documentation: [RELEASE.md](./RELEASE.md)

OpenLinker Core 从 `main` 发布，前提是 CI 和本地发布检查都通过。在公共 API 足够稳定
并采用严格语义化版本之前，重要变化记录在 [CHANGELOG.md](./CHANGELOG.md) 的
`Unreleased` 中。

## 发布前检查

1. 确认 `README.md`、`CONTRIBUTING.md`、`SECURITY.md`、`SUPPORT.md` 和示例是最新的。
2. 确认 `CHANGELOG.md` 描述了用户可见变化、迁移和兼容性说明。
3. 运行 `docker compose config -q`。
4. 运行 `make test`。
5. 如果本地缩小了 `make test` 范围，再运行 `go test ./...`。
6. 运行 `go vet ./...` 或 `make fmt`。
7. 在干净 checkout 上运行源码 secret scan，例如 `gitleaks dir --redact .`。
8. 确认生成产物、`.env`、覆盖率输出、本地二进制和临时日志没有被跟踪。
9. 确认生产说明没有建议 `ALLOW_LOCAL_HTTP_ENDPOINTS=true`。

## 打 tag

当 API 稳定到适合版本化消费者时，使用语义化版本 tag：

```bash
git tag v0.x.y
git push origin v0.x.y
```

pre-1.0 版本可以包含 breaking change，但必须在 `CHANGELOG.md` 中说明。

发布前必须确认没有真实 `.env`、token、客户数据或本地构建产物进入仓库。
