package wallet

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeWalletExec struct {
	tag  pgconn.CommandTag
	sql  string
	args []any
}

func (f *fakeWalletExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.sql = sql
	f.args = args
	return f.tag, nil
}

func TestChargeUsesBalanceGuard(t *testing.T) {
	userID := uuid.New()
	exec := &fakeWalletExec{tag: pgconn.NewCommandTag("UPDATE 1")}
	ok, err := charge(context.Background(), exec, userID, 250)
	if err != nil || !ok {
		t.Fatalf("charge ok=%v err=%v", ok, err)
	}
	if !strings.Contains(exec.sql, "balance_cents >= $2") {
		t.Fatalf("charge SQL missing balance guard: %s", exec.sql)
	}
	if len(exec.args) != 2 || exec.args[0] != userID || exec.args[1] != int64(250) {
		t.Fatalf("charge args = %#v", exec.args)
	}

	exec = &fakeWalletExec{tag: pgconn.NewCommandTag("UPDATE 0")}
	ok, err = charge(context.Background(), exec, userID, 999)
	if err != nil || ok {
		t.Fatalf("insufficient charge ok=%v err=%v", ok, err)
	}
}

func TestChargeRejectsNegativeAndAllowsZeroNoop(t *testing.T) {
	userID := uuid.New()
	exec := &fakeWalletExec{tag: pgconn.NewCommandTag("UPDATE 1")}
	if ok, err := charge(context.Background(), exec, userID, -1); err == nil || ok {
		t.Fatalf("negative charge ok=%v err=%v, want error", ok, err)
	}
	if ok, err := charge(context.Background(), exec, userID, 0); err != nil || !ok {
		t.Fatalf("zero charge ok=%v err=%v, want noop success", ok, err)
	}
}

func TestRefundClampsTotalSpent(t *testing.T) {
	userID := uuid.New()
	exec := &fakeWalletExec{tag: pgconn.NewCommandTag("UPDATE 1")}
	if err := refund(context.Background(), exec, userID, 125); err != nil {
		t.Fatalf("refund error = %v", err)
	}
	if !strings.Contains(exec.sql, "GREATEST(total_spent_cents - $2, 0)") {
		t.Fatalf("refund SQL does not clamp total_spent: %s", exec.sql)
	}
	if len(exec.args) != 2 || exec.args[0] != userID || exec.args[1] != int64(125) {
		t.Fatalf("refund args = %#v", exec.args)
	}
}

func TestRefundRejectsNegativeAndAllowsZeroNoop(t *testing.T) {
	userID := uuid.New()
	exec := &fakeWalletExec{tag: pgconn.NewCommandTag("UPDATE 1")}
	if err := refund(context.Background(), exec, userID, -1); err == nil {
		t.Fatal("negative refund should fail")
	}
	if err := refund(context.Background(), exec, userID, 0); err != nil {
		t.Fatalf("zero refund should be noop: %v", err)
	}
}
