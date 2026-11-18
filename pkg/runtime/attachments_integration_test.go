package runtime_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/OpenLinker-ai/openlinker-core/pkg/attachment"
	"github.com/OpenLinker-ai/openlinker-core/pkg/httpx"
	"github.com/OpenLinker-ai/openlinker-core/pkg/runtime"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	db "github.com/OpenLinker-ai/openlinker-core/pkg/db/generated"
)

func attachmentTestDir(t *testing.T) string {
	t.Helper()
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skip(err)
	}
	parent, err := os.MkdirTemp(base, "openlinker-run-attachments-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(parent) })
	require.NoError(t, os.Chmod(parent, 0o700))
	return filepath.Join(parent, "attachments")
}

// The production Run creation path checks ownership, expiry and single use,
// binds the attachment, and the sweeper releases it once the Run is terminal.
func TestRunCreationBindsAndReleasesAttachments(t *testing.T) {
	pool, svc, _, _, target := dryRunReadyFixture(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO agent_capabilities(agent_id,input_schema,output_schema)
VALUES($1,'{"type":"object","properties":{"image_url":{"type":"string","format":"uri"},"evidence_file_urls":{"type":"array","items":{"type":"string"}}},"additionalProperties":false}'::jsonb,'{"type":"object"}'::jsonb)`, target.ID)
	require.NoError(t, err)
	store, err := attachment.Open(attachment.Config{Dir: attachmentTestDir(t), MinFreeBytes: -1})
	require.NoError(t, err)
	defer store.Close()
	links, err := attachment.NewLinks("https://core.example")
	require.NoError(t, err)
	svc.SetRunAttachments(attachment.RunBinder{Store: store, Links: links})

	owner := target.CreatorID
	image, err := store.Put(ctx, owner, "cat.png", bytes.NewReader(append([]byte("\x89PNG\r\n\x1a\n"), 1, 2, 3)))
	require.NoError(t, err)
	doc, err := store.Put(ctx, owner, "report.pdf", bytes.NewReader([]byte("%PDF-1.7")))
	require.NoError(t, err)
	stranger, err := store.Put(ctx, uuid.New(), "x.png", bytes.NewReader(append([]byte("\x89PNG\r\n\x1a\n"), 9)))
	require.NoError(t, err)

	start := func(key string, input map[string]any) (*runtime.RunResponse, error) {
		return svc.StartRun(ctx, owner, &runtime.RunRequest{AgentID: target.ID.String(), Input: input, IdempotencyKey: key}, "web")
	}
	expectCode := func(err error, code string) {
		t.Helper()
		var httpErr *httpx.HTTPError
		require.True(t, errors.As(err, &httpErr), "%v", err)
		require.Equal(t, httpx.ErrorCode(code), httpErr.Code)
	}
	_, err = start("attach-stranger", map[string]any{"image_url": links.Link(stranger.Token)})
	expectCode(err, attachment.CodeNotOwned)

	run, err := start("attach-ok", map[string]any{
		"image_url": links.Link(image.Token), "evidence_file_urls": []any{links.Link(doc.Token), "https://cdn.example.org/a.pdf"},
	})
	require.NoError(t, err)
	runID := uuid.MustParse(run.RunID)
	for _, token := range []string{image.Token, doc.Token} {
		meta, err := store.Lookup(token)
		require.NoError(t, err)
		require.Equal(t, run.RunID, meta.RunID)
	}
	// Single use: a second Run cannot reuse a bound attachment.
	_, err = start("attach-reuse", map[string]any{"image_url": links.Link(image.Token)})
	expectCode(err, attachment.CodeExpired)

	queries := db.New(pool)
	status := func(ctx context.Context, id uuid.UUID) (string, bool, error) {
		found, err := queries.GetRunByID(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return found.Status, err == nil, err
	}
	result, err := store.Sweep(ctx, status, runtime.IsTerminalRunStatus)
	require.NoError(t, err)
	require.Zero(t, result.ReleasedRuns, "a running Run keeps its attachments")
	_, err = svc.CancelRun(ctx, owner, runID)
	require.NoError(t, err)
	current, _, err := status(ctx, runID)
	require.NoError(t, err)
	require.True(t, runtime.IsTerminalRunStatus(current), "queued Run cancel must be terminal, got %s", current)
	result, err = store.Sweep(ctx, status, runtime.IsTerminalRunStatus)
	require.NoError(t, err)
	require.Equal(t, 1, result.ReleasedRuns)
	for _, token := range []string{image.Token, doc.Token} {
		_, err := store.Lookup(token)
		require.ErrorIs(t, err, attachment.ErrNotFound)
	}
}
