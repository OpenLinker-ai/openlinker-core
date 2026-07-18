.PHONY: help dev build run bootstrap-admin runtime-node-issue runtime-node-inspect test lint fmt sqlc migrate-up migrate-down migrate-create migrate-status migration-063-test migration-069-test migration-070-test migration-071-test migration-074-test migration-075-test migration-076-test migration-077-test migration-078-test migration-079-test deps runtime-loadtest

ENV_FILE ?= .env
API_URL ?= http://localhost:8080
MIGRATE ?= go run -tags postgres github.com/golang-migrate/migrate/v4/cmd/migrate@v4.19.1

help: ## 显示所有命令
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

deps: ## 下载依赖
	go mod download
	go mod tidy

dev: ## 启动 hot-reload(需 air)
	@command -v air >/dev/null 2>&1 || { echo "请先安装 air: go install github.com/cosmtrek/air@latest"; exit 1; }
	air

build: ## 编译二进制到 bin/api
	mkdir -p bin
	go build -o bin/api ./cmd/api

run: build ## 编译并运行
	@set -a; . ./$(ENV_FILE); set +a; ./bin/api

bootstrap-admin: build ## 创建或提升首个管理员; 设置 OPENLINKER_BOOTSTRAP_ADMIN_PASSWORD
	@set -a; . ./$(ENV_FILE); set +a; ./bin/api bootstrap-admin

runtime-node-issue: build ## 离线签发并登记 Runtime Node; 参数放在 RUNTIME_NODE_ARGS
	@set -a; . ./$(ENV_FILE); set +a; ./bin/api runtime-node issue $(RUNTIME_NODE_ARGS)

runtime-node-inspect: build ## 审计 Runtime Node 证书; 参数放在 RUNTIME_NODE_ARGS
	@./bin/api runtime-node inspect $(RUNTIME_NODE_ARGS)

test: ## 运行测试(race + cover)
	go test ./... -race -cover

runtime-loadtest: ## 通过 WebSocket/长轮询回退对已启动 Core API 压测 Runtime Worker; 用 RUNTIME_LOADTEST_ARGS 覆盖参数
	go run ./cmd/runtime-loadtest -api $(API_URL)/api/v1 $(RUNTIME_LOADTEST_ARGS)

lint: ## golangci-lint
	@command -v golangci-lint >/dev/null 2>&1 || { echo "请先安装 golangci-lint"; exit 1; }
	golangci-lint run

fmt: ## 格式化
	gofmt -s -w .
	go vet ./...

sqlc: ## 重新生成 sqlc 代码(注意:pkg/db/generated/*.sql.go 是手写,谨慎覆盖)
	@command -v sqlc >/dev/null 2>&1 || { echo "请先安装 sqlc"; exit 1; }
	sqlc generate

migrate-up: ## 应用所有 migration(本模块自带 migrations/)
	@. ./$(ENV_FILE) && $(MIGRATE) -database "$$DATABASE_URL" -path migrations up

migrate-down: ## 回退一步
	@. ./$(ENV_FILE) && $(MIGRATE) -database "$$DATABASE_URL" -path migrations down 1

migrate-create: ## 创建 migration: make migrate-create name=add_xxx
	$(MIGRATE) create -ext sql -dir migrations -seq $(name)

migrate-status: ## 查看版本
	@. ./$(ENV_FILE) && $(MIGRATE) -database "$$DATABASE_URL" -path migrations version

migration-063-test: ## 在一次性 PostgreSQL 16 中验证 063 升级、回退、阻断条件与不变量
	./bin/test-migration-063.sh

migration-069-test: ## 在一次性 PostgreSQL 16 中验证 Runtime 统一入口契约升级、回退与阻断条件
	./bin/test-migration-069.sh

migration-070-test: ## 在一次性 PostgreSQL 16 中验证 SDK-first Runtime 边界硬切、回退与阻断条件
	./bin/test-migration-070.sh

migration-071-test: ## 在一次性 PostgreSQL 16 中验证 Runtime attachment generation 升级、回退与阻断条件
	./bin/test-migration-071.sh

migration-074-test: ## 在一次性 PostgreSQL 16 中验证通用 External Execution 原子切换与回退
	./bin/test-migration-074.sh

migration-075-test: ## 在一次性 PostgreSQL 16 中验证 Runtime N/N-1 升级、完整回退与重放
	./bin/test-migration-075.sh

migration-076-test: ## 在一次性 PostgreSQL 16 中验证 cancellation terminal reaper 升级、回退与重放
	./bin/test-migration-076.sh

migration-077-test: ## 在一次性 PostgreSQL 16 中验证 External Execution cancel/launch fence 升级与 fail-closed 回退
	./bin/test-migration-077.sh

migration-078-test: ## 在一次性 PostgreSQL 16 中验证 OAuth subject-only 升级、兼容读写与 fail-closed 回退
	./bin/test-migration-078.sh

migration-079-test: ## 在一次性 PostgreSQL 16 中验证 Runtime Attempt 传输证据升级、约束与 fail-closed 回退
	./bin/test-migration-079.sh
