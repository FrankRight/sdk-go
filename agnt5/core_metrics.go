package agnt5

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"
)

// coreMetrics emits opt-in benchmark observations. Execution identity travels
// with its context; no global run registry or scheduling lock is needed.
type coreMetrics struct {
	workerID, instanceID string
	epoch                time.Time
	sequence             atomic.Uint64
	logger               *log.Logger
}

type coreExecution struct {
	metrics            *coreMetrics
	runID, executionID string
	slotID             uint32
}

type coreExecutionKey struct{}

func newCoreMetrics(workerID string, output io.Writer) *coreMetrics {
	return &coreMetrics{workerID: workerID, instanceID: newWorkerID() + ":" + workerID,
		epoch: time.Now(), logger: log.New(output, "AGNT5_CORE_METRIC ", 0)}
}

func (m *coreMetrics) nowMS() float64 {
	return float64(m.epoch.UnixNano())/1e6 + float64(time.Since(m.epoch))/float64(time.Millisecond)
}

func (m *coreMetrics) emit(event map[string]any) {
	event["schema_version"] = 1
	event["worker_id"] = m.workerID
	event["instance_id"] = m.instanceID
	data, err := json.Marshal(event)
	if err == nil {
		_ = m.logger.Output(2, string(data))
	}
}

func (m *coreMetrics) configured(maxSlots uint32) {
	if m != nil {
		m.emit(map[string]any{"event": "configured", "at_ms": m.nowMS(), "max_slots": maxSlots})
	}
}

func (m *coreMetrics) claim(ctx context.Context, runID string, slot uint32) context.Context {
	if m == nil {
		return ctx
	}
	e := &coreExecution{metrics: m, runID: runID, slotID: slot,
		executionID: fmt.Sprintf("%s:%d", m.workerID, m.sequence.Add(1))}
	e.event("claimed")
	return context.WithValue(ctx, coreExecutionKey{}, e)
}

func executionMetrics(ctx context.Context) *coreExecution {
	e, _ := ctx.Value(coreExecutionKey{}).(*coreExecution)
	return e
}

func (e *coreExecution) event(name string) {
	if e == nil {
		return
	}
	e.metrics.emit(map[string]any{"event": name, "at_ms": e.metrics.nowMS(),
		"run_id": e.runID, "slot_id": e.slotID, "execution_id": e.executionID})
}

type coreTimer struct {
	execution *coreExecution
	operation string
	startedMS float64
	outcome   string
}

func startCoreTimer(ctx context.Context, operation string) coreTimer {
	t := coreTimer{execution: executionMetrics(ctx), operation: operation, outcome: "error"}
	if t.execution != nil {
		t.startedMS = t.execution.metrics.nowMS()
	}
	return t
}

func (t *coreTimer) finish(ctx context.Context, err error) {
	if t.execution == nil {
		return
	}
	if err != nil {
		t.outcome = "error"
		if ctx.Err() != nil {
			t.outcome = "cancelled"
		}
	}
	end := t.execution.metrics.nowMS()
	event := "activation_rpc"
	if t.operation == "business" {
		event = "business"
	}
	t.execution.metrics.emit(map[string]any{"event": event, "operation": t.operation, "outcome": t.outcome,
		"run_id": t.execution.runID, "execution_id": t.execution.executionID,
		"start_ms": t.startedMS, "end_ms": end, "elapsed_ms": end - t.startedMS, "at_ms": end})
}
