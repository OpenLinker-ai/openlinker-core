package httpx

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// streamEvents serves count events, one every interval, through w.
func streamEvents(w http.ResponseWriter, count int, interval time.Duration) {
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for i := 0; i < count; i++ {
		time.Sleep(interval)
		if _, err := w.Write([]byte("data: tick\n\n")); err != nil {
			return
		}
		flusher.Flush()
	}
	_, _ = w.Write([]byte("data: done\n\n"))
	flusher.Flush()
}

func readEvents(t *testing.T, url string) []string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET stream: %v", err)
	}
	defer resp.Body.Close()
	return readBodyEvents(resp)
}

func readBodyEvents(resp *http.Response) []string {
	var events []string
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "data: ") {
			events = append(events, strings.TrimPrefix(line, "data: "))
		}
	}
	return events
}

func newTimeoutServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.WriteTimeout = 300 * time.Millisecond
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func TestStreamingResponseWriterOutlivesServerWriteTimeout(t *testing.T) {
	srv := newTimeoutServer(t, func(w http.ResponseWriter, _ *http.Request) {
		streamEvents(NewStreamingResponseWriter(w, 200*time.Millisecond), 8, 100*time.Millisecond)
	})

	events := readEvents(t, srv.URL)
	if len(events) != 9 || events[len(events)-1] != "done" {
		t.Fatalf("stream ended early after %d events: %v", len(events), events)
	}
}

// Documents the failure the wrapper exists for: an unwrapped stream is cut off
// once the server write timeout passes, even though it writes every 100ms.
func TestUnwrappedStreamIsCutAtServerWriteTimeout(t *testing.T) {
	srv := newTimeoutServer(t, func(w http.ResponseWriter, _ *http.Request) {
		streamEvents(w, 8, 100*time.Millisecond)
	})

	events := readEvents(t, srv.URL)
	if len(events) == 9 && events[len(events)-1] == "done" {
		t.Fatalf("expected the server write timeout to cut the unwrapped stream, got all events")
	}
}

func TestStreamingResponseWriterKeepsFlusherAndUnwrap(t *testing.T) {
	rec := httptest.NewRecorder()
	w := NewStreamingResponseWriter(rec, 0)
	if _, ok := w.(http.Flusher); !ok {
		t.Fatal("wrapped writer must implement http.Flusher")
	}
	if _, err := w.Write([]byte("data: x\n\n")); err != nil {
		t.Fatalf("write through recorder: %v", err)
	}
	w.(http.Flusher).Flush()
	if !rec.Flushed || rec.Body.String() != "data: x\n\n" {
		t.Fatalf("recorder flushed=%v body=%q", rec.Flushed, rec.Body.String())
	}
	if got := w.(interface{ Unwrap() http.ResponseWriter }).Unwrap(); got != rec {
		t.Fatalf("Unwrap() = %T, want the original recorder", got)
	}
}

func TestHoldWriteDeadlineLetsLateResponseOutliveServerWriteTimeout(t *testing.T) {
	srv := newTimeoutServer(t, func(w http.ResponseWriter, _ *http.Request) {
		release := HoldWriteDeadline(w, 200*time.Millisecond)
		time.Sleep(700 * time.Millisecond) // a blocking call longer than WriteTimeout
		release()
		_, _ = w.Write([]byte("data: done\n\n"))
	})

	events := readEvents(t, srv.URL)
	if len(events) != 1 || events[0] != "done" {
		t.Fatalf("late response was lost: %v", events)
	}
}

// Documents the failure HoldWriteDeadline exists for.
func TestLateResponseIsLostAfterServerWriteTimeout(t *testing.T) {
	srv := newTimeoutServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(700 * time.Millisecond)
		_, _ = w.Write([]byte("data: done\n\n"))
	})

	resp, err := http.Get(srv.URL)
	if err == nil {
		defer resp.Body.Close()
		if events := readBodyEvents(resp); len(events) == 1 && events[0] == "done" {
			t.Fatal("expected the server write timeout to lose the late response")
		}
	}
}

func TestHoldWriteDeadlineReleaseIsIdempotent(t *testing.T) {
	release := HoldWriteDeadline(httptest.NewRecorder(), time.Second)
	release()
	release()
}
