package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type runtimeWorkerSpec struct {
	agent       agentRef
	token       string
	workerIndex int
}

type runningRuntimeWorker struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

type connectionCapacityStage struct {
	Phase             string    `json:"phase"`
	Target            int       `json:"target"`
	StartedAt         time.Time `json:"started_at"`
	ReadyAt           time.Time `json:"ready_at,omitempty"`
	EndedAt           time.Time `json:"ended_at"`
	Ready             int64     `json:"ready"`
	ConnectedMinimum  int64     `json:"connected_minimum"`
	ConnectedEnd      int64     `json:"connected_end"`
	WorkerErrorsDelta int64     `json:"worker_errors_delta"`
	HealthSamples     int       `json:"health_samples"`
	HealthFailures    int       `json:"health_failures"`
	Accepted          bool      `json:"accepted"`
	Error             string    `json:"error,omitempty"`
}

type connectionCapacityReport struct {
	Enabled                bool                      `json:"enabled"`
	RequestedConnections   int                       `json:"requested_connections"`
	StepSize               int                       `json:"step_size"`
	StepHoldMS             float64                   `json:"step_hold_ms"`
	ConnectStaggerMS       float64                   `json:"connect_stagger_ms"`
	CandidateConnections   int                       `json:"candidate_connections"`
	StableConnections      int                       `json:"measured_stable_connections"`
	FirstRejectedTarget    int                       `json:"first_rejected_target,omitempty"`
	RecommendedConnections int                       `json:"recommended_connections"`
	ConfirmationPassed     bool                      `json:"confirmation_passed"`
	FinalConnected         int64                     `json:"final_connected"`
	Stages                 []connectionCapacityStage `json:"stages"`
}

func makeRuntimeWorkerSpecs(agents []agentRef) []runtimeWorkerSpec {
	total := 0
	for _, agent := range agents {
		total += len(agent.RuntimeKeys)
	}
	specs := make([]runtimeWorkerSpec, 0, total)
	for _, agent := range agents {
		for workerIndex, token := range agent.RuntimeKeys {
			specs = append(specs, runtimeWorkerSpec{
				agent: agent, token: token, workerIndex: workerIndex,
			})
		}
	}
	return specs
}

func startRuntimeWorker(
	ctx context.Context,
	cfg config,
	spec runtimeWorkerSpec,
	tracker *runTracker,
	metrics *metrics,
	wg *sync.WaitGroup,
) (runningRuntimeWorker, error) {
	worker, err := newRuntimeWorker(cfg, spec.agent, spec.token, spec.workerIndex, tracker, metrics)
	if err != nil {
		return runningRuntimeWorker{}, err
	}
	workerCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		worker.run(workerCtx)
	}()
	return runningRuntimeWorker{cancel: cancel, done: done}, nil
}

func runConnectionCapacityStages(
	ctx context.Context,
	cfg config,
	coreAPI *apiClient,
	specs []runtimeWorkerSpec,
	tracker *runTracker,
	metrics *metrics,
	wg *sync.WaitGroup,
) (*connectionCapacityReport, error) {
	report := &connectionCapacityReport{
		Enabled: true, RequestedConnections: len(specs), StepSize: cfg.ConnectionStepSize,
		StepHoldMS: ms(cfg.ConnectionStepHold), ConnectStaggerMS: ms(cfg.ConnectStagger),
		Stages: make([]connectionCapacityStage, 0, (len(specs)+cfg.ConnectionStepSize-1)/cfg.ConnectionStepSize),
	}
	stable := 0
	for batchStart := 0; batchStart < len(specs); {
		target := min(batchStart+cfg.ConnectionStepSize, len(specs))
		stage := connectionCapacityStage{
			Phase: "staircase", Target: target, StartedAt: time.Now().UTC(),
			ConnectedMinimum: metrics.c.workersConnected.Load(),
		}
		workerErrorsBefore := metrics.c.workerErrors.Load()
		batch := make([]runningRuntimeWorker, 0, target-batchStart)
		var stageErr error
		for index := batchStart; index < target; index++ {
			running, err := startRuntimeWorker(ctx, cfg, specs[index], tracker, metrics, wg)
			if err != nil {
				stageErr = fmt.Errorf("start Runtime worker %d: %w", index, err)
				break
			}
			batch = append(batch, running)
			if index+1 < target && cfg.ConnectStagger > 0 {
				sleepContext(ctx, cfg.ConnectStagger)
				if ctx.Err() != nil {
					stageErr = ctx.Err()
					break
				}
			}
		}
		if stageErr == nil {
			stageErr = waitForWorkersReady(ctx, cfg, metrics, target)
		}
		if stageErr == nil {
			stage.ReadyAt = time.Now().UTC()
			stageErr = observeConnectionCapacityStage(ctx, cfg.ConnectionStepHold, target, coreAPI, metrics, &stage)
		}
		stage.Ready = metrics.c.workersReady.Load()
		stage.ConnectedEnd = metrics.c.workersConnected.Load()
		stage.WorkerErrorsDelta = metrics.c.workerErrors.Load() - workerErrorsBefore
		stage.EndedAt = time.Now().UTC()
		stage.Accepted = stageErr == nil
		if stageErr != nil {
			stage.Error = stageErr.Error()
		}
		report.Stages = append(report.Stages, stage)
		printConnectionCapacityStage(stage)
		if stageErr != nil {
			report.FirstRejectedTarget = target
			stopRuntimeWorkerBatch(batch, cfg.ReadyTimeout)
			report.CandidateConnections = stable
			report.FinalConnected = metrics.c.workersConnected.Load()
			if stable == 0 {
				return report, stageErr
			}
			return report, nil
		}
		stable = target
		report.CandidateConnections = stable
		batchStart = target
	}
	report.FinalConnected = metrics.c.workersConnected.Load()
	return report, nil
}

func confirmConnectionCapacity(
	ctx context.Context,
	duration time.Duration,
	coreAPI *apiClient,
	metrics *metrics,
	report *connectionCapacityReport,
) error {
	if report == nil || report.CandidateConnections <= 0 {
		return fmt.Errorf("no candidate connection capacity is available for confirmation")
	}
	stage := connectionCapacityStage{
		Phase: "confirmation", Target: report.CandidateConnections, StartedAt: time.Now().UTC(),
		ConnectedMinimum: metrics.c.workersConnected.Load(),
	}
	stage.ReadyAt = stage.StartedAt
	workerErrorsBefore := metrics.c.workerErrors.Load()
	err := observeConnectionCapacityStage(
		ctx, duration, report.CandidateConnections, coreAPI, metrics, &stage,
	)
	stage.Ready = metrics.c.workersReady.Load()
	stage.ConnectedEnd = metrics.c.workersConnected.Load()
	stage.WorkerErrorsDelta = metrics.c.workerErrors.Load() - workerErrorsBefore
	stage.EndedAt = time.Now().UTC()
	stage.Accepted = err == nil
	if err != nil {
		stage.Error = err.Error()
	}
	report.Stages = append(report.Stages, stage)
	printConnectionCapacityStage(stage)
	report.FinalConnected = stage.ConnectedEnd
	report.ConfirmationPassed = err == nil
	if err == nil {
		report.StableConnections = report.CandidateConnections
		report.RecommendedConnections = recommendedConnectionCapacity(
			report.StableConnections, report.StepSize,
		)
	}
	return err
}

func printConnectionCapacityStage(stage connectionCapacityStage) {
	fmt.Fprintf(
		os.Stderr,
		"runtime-loadtest capacity phase=%s target=%d ready=%d connected=%d min_connected=%d health_failures=%d worker_errors=%d accepted=%t error=%q\n",
		stage.Phase,
		stage.Target,
		stage.Ready,
		stage.ConnectedEnd,
		stage.ConnectedMinimum,
		stage.HealthFailures,
		stage.WorkerErrorsDelta,
		stage.Accepted,
		stage.Error,
	)
}

func observeConnectionCapacityStage(
	ctx context.Context,
	duration time.Duration,
	target int,
	coreAPI *apiClient,
	metrics *metrics,
	stage *connectionCapacityStage,
) error {
	if duration <= 0 {
		duration = time.Second
	}
	interval := time.Second
	if duration < 3*interval {
		interval = duration / 3
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}
	}
	minimum := int64(minimumStableConnections(target))
	stage.ConnectedMinimum = metrics.c.workersConnected.Load()
	consecutiveFailures := 0
	sample := func() error {
		stage.HealthSamples++
		connected := metrics.c.workersConnected.Load()
		if connected < stage.ConnectedMinimum {
			stage.ConnectedMinimum = connected
		}
		healthErr := probeCoreHealth(ctx, coreAPI)
		if healthErr != nil {
			stage.HealthFailures++
		}
		if connected < minimum || healthErr != nil {
			consecutiveFailures++
		} else {
			consecutiveFailures = 0
		}
		if consecutiveFailures >= 2 {
			return fmt.Errorf(
				"capacity stage unstable: connected=%d minimum=%d health_error=%v",
				connected, minimum, healthErr,
			)
		}
		return nil
	}
	if err := sample(); err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	deadline := time.NewTimer(duration)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := sample(); err != nil {
				return err
			}
		case <-deadline.C:
			connected := metrics.c.workersConnected.Load()
			stage.ConnectedEnd = connected
			if connected < minimum {
				return fmt.Errorf("capacity stage ended below stability threshold: connected=%d minimum=%d", connected, minimum)
			}
			return nil
		}
	}
}

func probeCoreHealth(ctx context.Context, api *apiClient) error {
	if api == nil || api.client == nil {
		return fmt.Errorf("Core API client is unavailable")
	}
	base := strings.TrimRight(api.root, "/")
	base = strings.TrimSuffix(base, "/api/v1")
	for _, path := range []string{"/healthz", "/readyz"} {
		requestCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, base+path, nil)
		if err != nil {
			cancel()
			return err
		}
		res, err := api.client.Do(req)
		if err != nil {
			cancel()
			return fmt.Errorf("GET %s: %w", path, err)
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
		_ = res.Body.Close()
		cancel()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("GET %s returned %d", path, res.StatusCode)
		}
	}
	return nil
}

func stopRuntimeWorkerBatch(workers []runningRuntimeWorker, timeout time.Duration) {
	for _, worker := range workers {
		if worker.cancel != nil {
			worker.cancel()
		}
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for _, worker := range workers {
		select {
		case <-worker.done:
		case <-deadline.C:
			return
		}
	}
}

func minimumStableConnections(target int) int {
	if target <= 0 {
		return 0
	}
	return (target*99 + 99) / 100
}

func recommendedConnectionCapacity(stable, step int) int {
	if stable <= 0 {
		return 0
	}
	withHeadroom := stable * 80 / 100
	if step <= 0 || withHeadroom < step {
		return withHeadroom
	}
	return withHeadroom / step * step
}

func minimumConnectionCapacityTimeout(cfg config) time.Duration {
	total := cfg.Agents * cfg.WorkersPerAgent
	stages := 0
	if total > 0 && cfg.ConnectionStepSize > 0 {
		stages = (total + cfg.ConnectionStepSize - 1) / cfg.ConnectionStepSize
	}
	connectDuration := time.Duration(max(total-1, 0)) * cfg.ConnectStagger
	return connectDuration + time.Duration(stages)*cfg.ConnectionStepHold +
		cfg.HoldAfter + cfg.ReadyTimeout + 2*time.Minute
}
