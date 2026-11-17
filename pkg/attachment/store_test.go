package attachment

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var png = append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{7}, 64)...)

func privateDir(t *testing.T) string {
	t.Helper()
	// t.TempDir lives under the system temp dir, which Open refuses on
	// purpose; use a private directory under the user's home cache instead.
	base, err := os.UserCacheDir()
	if err != nil {
		t.Skip(err)
	}
	parent, err := os.MkdirTemp(base, "openlinker-attachment-test-")
	if err != nil {
		t.Skip(err)
	}
	t.Cleanup(func() { os.RemoveAll(parent) })
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(parent, "attachments")
}

func openStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = privateDir(t)
	}
	cfg.MinFreeBytes = -1 // disable the disk threshold unless a test sets it
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestOpenRejectsUnsafeDirectories(t *testing.T) {
	for _, dir := range []string{"", "relative/attachments", "/", filepath.Join(os.TempDir(), "x")} {
		if _, err := Open(Config{Dir: dir}); err == nil {
			t.Errorf("%q accepted", dir)
		}
	}
	loose := privateDir(t)
	if err := os.Mkdir(loose, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{Dir: loose}); err == nil || !strings.Contains(err.Error(), "0700") {
		t.Fatalf("group/other-readable directory accepted: %v", err)
	}
	target := privateDir(t)
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := privateDir(t)
	if err := os.Symlink(target, link); err != nil {
		t.Skip(err)
	}
	if _, err := Open(Config{Dir: link}); err == nil {
		t.Fatal("symbolic link directory accepted")
	}
	fresh := privateDir(t)
	s, err := Open(Config{Dir: fresh})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	info, err := os.Stat(fresh)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("created directory mode %v %v", info.Mode(), err)
	}
}

func TestPutStoresPrivatelyAndServesByToken(t *testing.T) {
	s := openStore(t, Config{})
	owner := uuid.New()
	upload, err := s.Put(context.Background(), owner, "../../etc/passwd\x00.png", bytes.NewReader(png))
	if err != nil {
		t.Fatal(err)
	}
	if upload.Kind != KindImage || upload.MediaType != "image/png" || upload.Size != int64(len(png)) || !ValidToken(upload.Token) {
		t.Fatalf("upload %+v", upload)
	}
	if upload.Filename != "passwd.png" {
		t.Fatalf("filename %q", upload.Filename)
	}
	meta, file, err := s.Open(upload.Token)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(file)
	file.Close()
	if !bytes.Equal(data, png) || meta.ID != upload.ID {
		t.Fatal("content mismatch")
	}
	// Files are 0600 with random names; metadata never contains the token.
	err = filepath.WalkDir(s.Dir(), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		info, _ := entry.Info()
		if entry.IsDir() && info.Mode().Perm() != 0o700 {
			t.Errorf("directory %s mode %v", path, info.Mode())
		}
		if !entry.IsDir() {
			if info.Mode().Perm() != 0o600 {
				t.Errorf("file %s mode %v", path, info.Mode())
			}
			raw, _ := os.ReadFile(path)
			if bytes.Contains(raw, []byte(upload.Token)) || strings.Contains(path, upload.Token) {
				t.Errorf("%s exposes the capability token", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := s.Put(context.Background(), owner, "report.pdf", strings.NewReader("%PDF-1.7 any bytes"))
	if err != nil || doc.Kind != KindFile || doc.MediaType != "application/octet-stream" {
		t.Fatalf("document %+v %v", doc, err)
	}
	for _, token := range []string{"", "../x", strings.Repeat("A", 64), strings.Repeat("a", 64)} {
		if _, _, err := s.Open(token); !errors.Is(err, ErrNotFound) {
			t.Errorf("%q: %v", token, err)
		}
	}
}

func TestPutLimits(t *testing.T) {
	s := openStore(t, Config{OwnerMaxCount: 2, OwnerMaxBytes: 1 << 20, InstanceMaxBytes: 1 << 30})
	owner := uuid.New()
	if _, err := s.Put(context.Background(), owner, "big", io.LimitReader(zeroReader{}, MaxFileBytes+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized: %v", err)
	}
	if _, err := s.Put(context.Background(), owner, "empty", strings.NewReader("")); !errors.Is(err, ErrEmpty) {
		t.Fatalf("empty: %v", err)
	}
	if entries, _ := os.ReadDir(filepath.Join(s.Dir(), "tmp")); len(entries) != 0 {
		t.Fatalf("rejected uploads left temporary files: %v", entries)
	}
	for i := 0; i < 2; i++ {
		if _, err := s.Put(context.Background(), owner, "a", bytes.NewReader(png)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Put(context.Background(), owner, "a", bytes.NewReader(png)); !errors.Is(err, ErrOwnerQuota) {
		t.Fatalf("owner count: %v", err)
	}
	if _, err := s.Put(context.Background(), uuid.New(), "a", bytes.NewReader(png)); err != nil {
		t.Fatalf("another owner blocked: %v", err)
	}
	small := openStore(t, Config{InstanceMaxBytes: int64(len(png)) + 10})
	if _, err := small.Put(context.Background(), owner, "a", bytes.NewReader(png)); err != nil {
		t.Fatal(err)
	}
	if _, err := small.Put(context.Background(), uuid.New(), "a", bytes.NewReader(png)); !errors.Is(err, ErrInstanceFull) {
		t.Fatalf("instance limit: %v", err)
	}
	low := openStore(t, Config{})
	low.cfg.MinFreeBytes = 1 << 40
	low.freeSpace = func(string) (uint64, uint64, error) { return 1 << 30, 1 << 31, nil }
	if _, err := low.Put(context.Background(), owner, "a", bytes.NewReader(png)); !errors.Is(err, ErrDiskLow) {
		t.Fatalf("disk threshold: %v", err)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestCheckInputAndBind(t *testing.T) {
	s := openStore(t, Config{OwnerMaxCount: 100})
	links, err := NewLinks("https://example.com")
	if err != nil {
		t.Fatal(err)
	}
	owner, other := uuid.New(), uuid.New()
	mine, _ := s.Put(context.Background(), owner, "a.png", bytes.NewReader(png))
	doc, _ := s.Put(context.Background(), owner, "b.pdf", strings.NewReader("%PDF"))
	theirs, _ := s.Put(context.Background(), other, "c.png", bytes.NewReader(png))
	input := map[string]any{
		"image_url": links.Link(mine.Token), "evidence_file_urls": []any{links.Link(doc.Token), "https://cdn.example.org/x.pdf"},
		"note": links.Link(theirs.Token), // not an attachment field: ignored
	}
	ids, err := s.CheckInput(owner, links, input)
	if err != nil || len(ids) != 2 {
		t.Fatalf("ids %v %v", ids, err)
	}
	expectCode := func(input map[string]any, code string) {
		t.Helper()
		_, err := s.CheckInput(owner, links, input)
		var field *FieldError
		if !errors.As(err, &field) || field.Code != code {
			t.Fatalf("%v: %v, want %s", input, err, code)
		}
	}
	expectCode(map[string]any{"image_url": links.Link(theirs.Token)}, CodeNotOwned)
	expectCode(map[string]any{"file_url": links.Link(strings.Repeat("b", 64))}, CodeExpired)
	expectCode(map[string]any{"file_url": "https://example.com/api/v1/attachments/short"}, CodeExpired)
	expectCode(map[string]any{"file_urls": []any{1}}, CodeInvalid)
	many := []any{}
	for i := 0; i < MaxPerRun+1; i++ {
		many = append(many, "https://cdn.example.org/x.pdf")
	}
	expectCode(map[string]any{"file_urls": many}, CodeLimit)

	run := uuid.New()
	if err := s.Bind(run, ids); err != nil {
		t.Fatal(err)
	}
	expectCode(map[string]any{"image_url": links.Link(mine.Token)}, CodeExpired) // single use
	meta, err := s.Lookup(mine.Token)
	if err != nil || meta.RunID != run.String() || meta.ExpiresAt.Before(time.Now().Add(DefaultBoundLifetime-time.Minute)) {
		t.Fatalf("bound meta %+v %v", meta, err)
	}
}

func TestSweepReleasesTerminalRunsExpiredAndOrphans(t *testing.T) {
	s := openStore(t, Config{OwnerMaxCount: 100, UnboundTTL: time.Hour})
	owner := uuid.New()
	running, done, gone := uuid.New(), uuid.New(), uuid.New()
	uploads := map[uuid.UUID]*Upload{}
	for _, run := range []uuid.UUID{running, done, gone} {
		u, err := s.Put(context.Background(), owner, "a.png", bytes.NewReader(png))
		if err != nil {
			t.Fatal(err)
		}
		uploads[run] = u
		if err := s.Bind(run, []string{u.ID}); err != nil {
			t.Fatal(err)
		}
	}
	unbound, _ := s.Put(context.Background(), owner, "late.png", bytes.NewReader(png))
	fresh, _ := s.Put(context.Background(), owner, "fresh.png", bytes.NewReader(png))
	// An orphan blob and a stale temporary file, both older than an hour.
	orphan := filepath.Join(s.Dir(), "blobs", "ff", strings.Repeat("f", 32))
	os.MkdirAll(filepath.Dir(orphan), 0o700)
	os.WriteFile(orphan, png, 0o600)
	stale := filepath.Join(s.Dir(), "tmp", "leftover")
	os.WriteFile(stale, png, 0o600)
	old := time.Now().Add(-2 * time.Hour)
	os.Chtimes(orphan, old, old)
	os.Chtimes(stale, old, old)

	// Expire the unbound upload by moving the clock.
	s.now = func() time.Time { return time.Now().Add(90 * time.Minute) }
	// keep "fresh" alive by refreshing its expiry
	meta, _ := s.readMeta(fresh.ID)
	meta.ExpiresAt = time.Now().Add(3 * time.Hour)
	s.writeMeta(meta)

	statuses := map[uuid.UUID]string{running: "running", done: "success"}
	result, err := s.Sweep(context.Background(), func(_ context.Context, run uuid.UUID) (string, bool, error) {
		status, ok := statuses[run]
		return status, ok, nil
	}, func(status string) bool { return status == "success" || status == "failed" || status == "timeout" || status == "canceled" })
	if err != nil {
		t.Fatal(err)
	}
	if result.ReleasedRuns != 2 || result.Expired != 1 || result.Orphans < 2 {
		t.Fatalf("result %+v", result)
	}
	if _, err := s.Lookup(uploads[running].Token); err != nil {
		t.Fatal("running Run attachment released")
	}
	for _, u := range []*Upload{uploads[done], uploads[gone], unbound} {
		if _, err := s.Lookup(u.Token); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s not released", u.ID)
		}
		if _, err := os.Stat(filepath.Join(s.Dir(), filepath.FromSlash(blobPath(u.ID)))); !os.IsNotExist(err) {
			t.Fatalf("%s contents remain", u.ID)
		}
	}
	if _, err := s.Lookup(fresh.Token); err != nil {
		t.Fatal("live unbound attachment released")
	}
	for _, path := range []string{orphan, stale} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s not cleaned", path)
		}
	}
	count, _ := s.ownerUsage(owner)
	if count != 2 {
		t.Fatalf("owner index after release: %d", count)
	}
}

func TestFieldConvention(t *testing.T) {
	for key, want := range map[string]bool{
		"image_url": true, "image_urls": true, "file_url": true, "file_urls": true, "cover_image_url": true,
		"evidence_file_urls": true, "_file_url": false, "url": false, "profile": false, "imageurl": false,
	} {
		if IsAttachmentField(key) != want {
			t.Errorf("%s = %t", key, !want)
		}
	}
}
