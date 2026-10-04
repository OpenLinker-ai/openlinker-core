package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"
)

const defaultCoreShutdownPhaseTimeout = 10 * time.Second

type coreShutdownFunc func(context.Context) error
type coreSessionDetachFunc func(context.Context) (int64, error)

type coreShutdownPlan struct {
	PhaseTimeout              time.Duration
	RuntimeAttachOnly         bool
	ShutdownRuntimeController coreShutdownFunc
	ShutdownHTTP              coreShutdownFunc
	ShutdownRuntimeMTLS       coreShutdownFunc
	DetachSessions            coreSessionDetachFunc
	ShutdownA2AGRPC           coreShutdownFunc
	CloseRuntimeCluster       coreShutdownFunc
}

type coreShutdownResult struct {
	DetachedSessions int64
	DetachCompleted  bool
}

func newCoreShutdownPlan(server *http.Server, attachOnly bool, closeCluster coreShutdownFunc) coreShutdownPlan {
	plan := coreShutdownPlan{
		PhaseTimeout:        defaultCoreShutdownPhaseTimeout,
		RuntimeAttachOnly:   attachOnly,
		CloseRuntimeCluster: closeCluster,
	}
	// Echo.StartServer serves the supplied server; Echo.Shutdown only drains
	// Echo's own default servers. The plan must own the actual listener.
	if server != nil {
		plan.ShutdownHTTP = server.Shutdown
	}
	return plan
}

// waitForCoreStop gives an already-reported fatal serve error priority over a
// concurrently delivered termination signal. The signal branch drains once so
// select's randomized choice cannot turn a fatal listener failure into exit 0.
func waitForCoreStop(signals <-chan os.Signal, serveErrors <-chan error) error {
	select {
	case err := <-serveErrors:
		return err
	default:
	}
	select {
	case err := <-serveErrors:
		return err
	case <-signals:
		select {
		case err := <-serveErrors:
			return err
		default:
			return nil
		}
	}
}

// runCoreShutdown is the strict, ordered process handoff. Each phase gets a
// fresh deadline so one exhausted shutdown cannot silently disable every later
// cleanup. Attach-only Sessions are detached only after both HTTP listeners and
// the hijacked WebSocket controller have confirmed that they are stopped.
func runCoreShutdown(plan coreShutdownPlan) (coreShutdownResult, error) {
	timeout := plan.PhaseTimeout
	if timeout <= 0 {
		timeout = defaultCoreShutdownPhaseTimeout
	}

	result := coreShutdownResult{}
	errs := make([]error, 0, 6)
	ingressStopped := true
	if plan.ShutdownRuntimeController == nil {
		ingressStopped = false
		errs = append(errs, errors.New("runtime websocket shutdown is unavailable"))
	} else if err := runCoreShutdownPhase(timeout, "runtime websocket shutdown", plan.ShutdownRuntimeController); err != nil {
		ingressStopped = false
		errs = append(errs, err)
	}
	if plan.ShutdownHTTP == nil {
		ingressStopped = false
		errs = append(errs, errors.New("HTTP shutdown is unavailable"))
	} else if err := runCoreShutdownPhase(timeout, "HTTP shutdown", plan.ShutdownHTTP); err != nil {
		ingressStopped = false
		errs = append(errs, err)
	}
	if plan.ShutdownRuntimeMTLS != nil {
		if err := runCoreShutdownPhase(timeout, "runtime mTLS shutdown", plan.ShutdownRuntimeMTLS); err != nil {
			ingressStopped = false
			errs = append(errs, err)
		}
	}

	if plan.RuntimeAttachOnly && ingressStopped {
		switch {
		case plan.DetachSessions == nil:
			errs = append(errs, errors.New("runtime attach-only Session detach is unavailable"))
		default:
			detached, err := runCoreSessionDetachPhase(timeout, plan.DetachSessions)
			if err != nil {
				errs = append(errs, err)
			} else {
				result.DetachedSessions = detached
				result.DetachCompleted = true
			}
		}
	}

	if plan.ShutdownA2AGRPC != nil {
		if err := runCoreShutdownPhase(timeout, "a2a gRPC shutdown", plan.ShutdownA2AGRPC); err != nil {
			errs = append(errs, err)
		}
	}
	if plan.CloseRuntimeCluster == nil {
		errs = append(errs, errors.New("runtime cluster shutdown is unavailable"))
	} else if err := runCoreShutdownPhase(timeout, "runtime cluster shutdown", plan.CloseRuntimeCluster); err != nil {
		errs = append(errs, err)
	}
	return result, errors.Join(errs...)
}

func runCoreShutdownPhase(timeout time.Duration, name string, shutdown coreShutdownFunc) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}

func runCoreSessionDetachPhase(timeout time.Duration, detach coreSessionDetachFunc) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	detached, err := detach(ctx)
	if err != nil {
		return 0, fmt.Errorf("runtime attach-only Session detach: %w", err)
	}
	return detached, nil
}
