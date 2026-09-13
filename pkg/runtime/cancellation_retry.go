package runtime

import "time"

// Only arm this timer after the authenticated database probe found pending
// commands. Each retry rechecks eligibility (including the database deadline),
// releases all transaction locks, and backs off from 25ms to at most 250ms.
// The transport loop owns the timer; no background callback outlives it.
type runtimeCancellationRetry struct {
	timer   *time.Timer
	attempt uint
}

func (r *runtimeCancellationRetry) wait() <-chan time.Time {
	delay := min(25*time.Millisecond<<r.attempt, 250*time.Millisecond)
	if r.attempt < 4 {
		r.attempt++
	}
	if r.timer == nil {
		r.timer = time.NewTimer(delay)
	} else {
		r.timer.Reset(delay)
	}
	return r.timer.C
}

func (r *runtimeCancellationRetry) stop() {
	if r.timer != nil {
		r.timer.Stop()
	}
}

func (r *runtimeCancellationRetry) reset() {
	r.stop()
	r.attempt = 0
}
