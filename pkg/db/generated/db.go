// Code generated manually as a placeholder for sqlc output.
// TODO: 由 sqlc 生成（运行 `sqlc generate` 后会覆盖此文件）。
// 包路径与 sqlc.yaml 中 `package: db` + `out: pkg/db/generated` 一致。

package db

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DBTX 抽象 pgx 连接接口，pgxpool.Pool 与 pgx.Tx 都实现它。
// 这样 Queries 既可走连接池，也可走事务（WithTx）。
type DBTX interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
}

// New 创建一个 Queries 实例。
func New(db DBTX) *Queries {
	return &Queries{db: db}
}

// Queries 持有 DBTX 引用，所有 SQL 方法定义在此结构上。
type Queries struct {
	db DBTX
}

// WithTx 返回绑定到给定事务的新 Queries 实例。
func (q *Queries) WithTx(tx pgx.Tx) *Queries {
	return &Queries{db: tx}
}
