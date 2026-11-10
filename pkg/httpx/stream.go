package httpx

import (
	"net/http"
	"sync"
	"time"
)

// StreamWriteWindow bounds how long one write of a long-lived stream may take.
// It must stay above every stream's heartbeat interval so an idle but healthy
// stream always writes again before its deadline.
const StreamWriteWindow = 60 * time.Second

// streamingResponseWriter moves the connection write deadline forward before
// every write and flush. http.Server.WriteTimeout is an absolute deadline
// fixed when the request starts, so without this a Server-Sent Events stream
// is cut off once that timeout passes, however often it sends heartbeats. A
// client that stops reading still fails within one window.
type streamingResponseWriter struct {
	http.ResponseWriter
	controller *http.ResponseController
	window     time.Duration
}

// NewStreamingResponseWriter wraps w for a long-lived streaming response. The
// result also implements http.Flusher when w does.
func NewStreamingResponseWriter(w http.ResponseWriter, window time.Duration) http.ResponseWriter {
	if window <= 0 {
		window = StreamWriteWindow
	}
	return &streamingResponseWriter{
		ResponseWriter: w,
		controller:     http.NewResponseController(w),
		window:         window,
	}
}

func (w *streamingResponseWriter) extendDeadline() {
	// Writers that cannot set deadlines (such as test recorders) have no
	// server timeout to outlive, so the error is safe to ignore.
	_ = w.controller.SetWriteDeadline(time.Now().Add(w.window))
}

func (w *streamingResponseWriter) Write(p []byte) (int, error) {
	w.extendDeadline()
	return w.ResponseWriter.Write(p)
}

func (w *streamingResponseWriter) Flush() {
	w.extendDeadline()
	_ = w.controller.Flush()
}

func (w *streamingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// HoldWriteDeadline keeps moving w's write deadline forward while a handler
// blocks before writing its response, for example a synchronous call that
// waits for a Run to finish. Without it the response is written after the
// server's absolute WriteTimeout has passed and fails (HTTP/2 resets the
// stream when that timer fires). Call the returned function before writing;
// it stops the refresh and leaves one full window for the response.
func HoldWriteDeadline(w http.ResponseWriter, window time.Duration) (release func()) {
	if window <= 0 {
		window = StreamWriteWindow
	}
	controller := http.NewResponseController(w)
	extend := func() { _ = controller.SetWriteDeadline(time.Now().Add(window)) }
	extend()
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(window / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				extend()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-stopped
			extend()
		})
	}
}
