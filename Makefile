.PHONY: help dev build run bootstrap-admin test lint fmt sqlc migrate-up migrate-down migrate-create migrate-status deps demo-a2a demo-a2a-live runtime-loadtest

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

test: ## 运行测试(race + cover)
	go test ./... -race -cover

demo-a2a: ## 对已启动的本地 API 跑真实 Agent-to-Agent 闭环
	go run ./cmd/a2a-demo -api $(API_URL)

demo-a2a-live: ## 注册并保持可从 Playground 反复调用的本地 Agent
	go run ./cmd/a2a-demo -api $(API_URL) -serve

runtime-loadtest: ## 对已启动 Core API 跑 runtime_ws/runtime_pull 压测; 用 RUNTIME_LOADTEST_ARGS 覆盖参数
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
