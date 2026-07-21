package runtime

// WorkerObservation is a bounded, payload-free marker for tests and load
// baselines. Category and reason are stable code values; BatchSize is a count,
// never a resource identifier. Production leaves the observer nil.
type WorkerObservation struct {
	Category  string
	Reason    string
	BatchSize int
}

// WorkerObserver must not perform database or network I/O. Implementations
// used by tests should be concurrency-safe and return immediately.
type WorkerObserver interface {
	ObserveWorker(WorkerObservation)
}

type WorkerObserverFunc func(WorkerObservation)

func (f WorkerObserverFunc) ObserveWorker(observation WorkerObservation) {
	if f != nil {
		f(observation)
	}
}

func observeWorker(observer WorkerObserver, category, reason string, batchSize int) {
	if observer == nil {
		return
	}
	if batchSize < 0 {
		batchSize = 0
	}
	observer.ObserveWorker(WorkerObservation{
		Category: category, Reason: reason, BatchSize: batchSize,
	})
}
