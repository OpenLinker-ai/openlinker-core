package runtime

import (
	"sync"
	"testing"
)

func TestWorkerObserverIsOptionalPayloadFreeAndConcurrencySafe(t *testing.T) {
	observeWorker(nil, "runtime.test", "nil", 1)

	var mu sync.Mutex
	observations := make([]WorkerObservation, 0, 64)
	observer := WorkerObserverFunc(func(observation WorkerObservation) {
		mu.Lock()
		observations = append(observations, observation)
		mu.Unlock()
	})
	var workers sync.WaitGroup
	for i := 0; i < 64; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			observeWorker(observer, "runtime.test", "concurrent", -1)
		}()
	}
	workers.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(observations) != 64 {
		t.Fatalf("observation count = %d, want 64", len(observations))
	}
	for _, observation := range observations {
		if observation.Category != "runtime.test" || observation.Reason != "concurrent" || observation.BatchSize != 0 {
			t.Fatalf("observation = %#v", observation)
		}
	}
}
