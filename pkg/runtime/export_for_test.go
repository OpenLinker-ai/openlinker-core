// Package runtime - 测试钩子（仅在测试二进制中编译）。
//
// 通过 internal-package 文件给 _test.go 暴露一个安全的 SetHTTPClient 入口，
// 让测试可以注入 httptest.NewTLSServer 自带的 *http.Client（含其自签 CA 信任）。
//
// 不在 service.go 主文件里加 setter，避免污染生产 API。

package runtime

import (
	"net/http"
	"time"

	"github.com/google/uuid"
)

// SetHTTPClient 仅供 _test.go 注入 client（如 httptest.NewTLSServer().Client()）。
// 生产代码不应调用。
func (s *Service) SetHTTPClient(c *http.Client) {
	s.httpClient = c
}

// DropLocalFramesForTest removes this process's frame buffer for a Run without
// touching the audit, standing in for a Core that restarted while an
// observation was still recorded as active.
func (observation *BrowserObservation) DropLocalFramesForTest(runID uuid.UUID) {
	observation.frames.close(runID)
}

// AgeLastPollForTest backdates when a Run's observation was last polled, so the
// abandonment grace can be exercised without waiting it out.
func (observation *BrowserObservation) AgeLastPollForTest(
	runID uuid.UUID,
	age time.Duration,
) {
	observation.frames.mu.Lock()
	defer observation.frames.mu.Unlock()
	if live := observation.frames.live[runID]; live != nil {
		live.lastPolledAt = live.lastPolledAt.Add(-age)
	}
}
