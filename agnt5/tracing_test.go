package agnt5

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestInvocationTelemetryTraceAndNestedScopes(t *testing.T) {
	logs := &recordingLogExporter{}
	spans := tracetest.NewInMemoryExporter()
	worker := NewWorker("traced", WithWorkerID("worker-1"))
	worker.telemetry = newTelemetry(worker, logs, spans)
	defer worker.shutdownTelemetry()
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(io.Discard, nil)))
	if err := RegisterWorkflow(worker, "nested", func(ctx *Context, input string) (string, error) {
		_, err := StepWithKey(ctx, "generate", "first", func(step *Context) (string, error) {
			logger.InfoContext(step, "step log")
			response, err := step.Generate(StaticModel{Model: "static", Content: input}, GenerateRequest{})
			return response.Content, err
		})
		ctx.Logger().Info("root log")
		return input, err
	}); err != nil {
		t.Fatal(err)
	}
	const traceID = "0123456789abcdef0123456789abcdef"
	const parentID = "0123456789abcdef"
	_, err := worker.invoke(context.Background(), Invocation{ID: "inv-1", RunID: "run-1", ComponentName: "nested", ComponentType: ComponentTypeWorkflow, Input: []byte(`"hello"`), Metadata: map[string]string{"traceparent": "00-" + traceID + "-" + parentID + "-01"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.telemetry.traceProvider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.telemetry.provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := spans.GetSpans()
	if len(got) != 3 {
		t.Fatalf("spans = %d, want invocation, step, model", len(got))
	}
	byName := make(map[string]tracetest.SpanStub)
	for _, span := range got {
		byName[span.Name] = span
		if span.SpanContext.TraceID().String() != traceID {
			t.Fatalf("wrong trace: %s", span.SpanContext.TraceID())
		}
	}
	root := byName["workflow.nested"]
	step := byName["workflow.step.generate"]
	model := byName["lm.static"]
	if root.Parent.SpanID().String() != parentID || step.Parent.SpanID() != root.SpanContext.SpanID() || model.Parent.SpanID() != step.SpanContext.SpanID() {
		t.Fatalf("bad span ancestry: %#v", got)
	}
	records := logs.Records()
	if len(records) != 2 {
		t.Fatalf("logs = %d, want 2", len(records))
	}
	for _, record := range records {
		wantSpan := root.SpanContext.SpanID()
		if record.Body().AsString() == "step log" {
			wantSpan = step.SpanContext.SpanID()
		}
		attrs := recordAttributes(record)
		if record.TraceID().String() != traceID || record.SpanID() != wantSpan || attrs["trace_id"] != traceID || attrs["span_id"] != wantSpan.String() || attrs["run_id"] != "run-1" {
			t.Fatalf("wrong log correlation: %#v", attrs)
		}
	}
}

func TestInvocationTelemetryFailureAndPanic(t *testing.T) {
	for _, mode := range []string{"error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			spans := tracetest.NewInMemoryExporter()
			worker := NewWorker("service")
			worker.telemetry = newTelemetry(worker, &recordingLogExporter{}, spans)
			defer worker.shutdownTelemetry()
			if err := RegisterFunction(worker, "fail", func(*Context, string) (string, error) {
				if mode == "panic" {
					panic("broken handler")
				}
				return "", errors.New("broken handler")
			}); err != nil {
				t.Fatal(err)
			}
			_, err := worker.invoke(context.Background(), Invocation{ID: "run-1", ComponentName: "fail", Input: []byte(`"hello"`)})
			if err == nil {
				t.Fatal("expected handler failure")
			}
			if err := worker.telemetry.traceProvider.ForceFlush(context.Background()); err != nil {
				t.Fatal(err)
			}
			got := spans.GetSpans()
			if len(got) != 1 || got[0].Status.Code != codes.Error || len(got[0].Events) != 1 {
				t.Fatalf("failure span = %#v", got)
			}
		})
	}
}

func TestInvocationTelemetryConcurrentRunIsolation(t *testing.T) {
	logs := &recordingLogExporter{}
	spans := tracetest.NewInMemoryExporter()
	worker := NewWorker("service")
	worker.telemetry = newTelemetry(worker, logs, spans)
	defer worker.shutdownTelemetry()
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(io.Discard, nil)))
	if err := RegisterFunction(worker, "log", func(ctx *Context, input string) (string, error) {
		derived, cancel := context.WithCancel(ctx.Context)
		defer cancel()
		logger.InfoContext(derived, input)
		return input, nil
	}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			id := fmt.Sprintf("run-%d", i)
			_, err := worker.invoke(context.Background(), Invocation{ID: id, RunID: id, ComponentName: "log", Input: []byte(fmt.Sprintf("%q", id))})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	logger.InfoContext(context.Background(), "outside invocation")
	if err := worker.telemetry.provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.telemetry.traceProvider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	records := logs.Records()
	if len(records) != 12 || len(spans.GetSpans()) != 12 {
		t.Fatalf("logs/spans = %d/%d, want 12 each", len(records), len(spans.GetSpans()))
	}
	traceIDs := make(map[trace.TraceID]bool)
	for _, record := range records {
		if recordAttributes(record)["agnt5.run.id"] != record.Body().AsString() {
			t.Fatal("cross-run attribution")
		}
		traceIDs[record.TraceID()] = true
	}
	if len(traceIDs) != 12 {
		t.Fatalf("unique traces = %d", len(traceIDs))
	}
}

func TestSlogPreservesGroupsFilteringAndLocalHandler(t *testing.T) {
	logs := &recordingLogExporter{}
	worker := NewWorker("service")
	worker.telemetry = newTelemetry(worker, logs)
	defer worker.shutdownTelemetry()
	ctx := newContext(context.Background(), Invocation{ID: "run-1"}, nil, "")
	ctx.setTelemetry(worker.telemetry)
	var output bytes.Buffer
	logger := slog.New(NewSlogHandler(slog.NewJSONHandler(&output, nil))).With("base", 1).WithGroup("request").With("id", 2)
	logger.DebugContext(ctx, "filtered")
	logger.InfoContext(ctx, "sent", slog.Group("nested", "value", 3))
	logger.InfoContext(context.Background(), "local only")
	if err := worker.telemetry.provider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	records := logs.Records()
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	attrs := recordAttributes(records[0])
	for key, want := range map[string]string{"field.base": "1", "field.request.id": "2", "field.request.nested.value": "3"} {
		if attrs[key] != want {
			t.Fatalf("%s = %q, want %q", key, attrs[key], want)
		}
	}
	if bytes.Contains(output.Bytes(), []byte("filtered")) || !bytes.Contains(output.Bytes(), []byte("sent")) || !bytes.Contains(output.Bytes(), []byte("local only")) {
		t.Fatalf("local handler output = %s", &output)
	}
}

func TestPullTraceIDIsPreservedWithoutInventedParent(t *testing.T) {
	spans := tracetest.NewInMemoryExporter()
	worker := NewWorker("service")
	worker.telemetry = newTelemetry(worker, &recordingLogExporter{}, spans)
	defer worker.shutdownTelemetry()
	const id = "0123456789abcdef0123456789abcdef"
	req := dispatchRequestFromJob(&pb.JobAssignment{RunId: "run-1", TraceId: id}, "service")
	ctx, finish := worker.startInvocationTelemetry(context.Background(), invocationFromDispatch(req))
	if got := trace.SpanContextFromContext(ctx).TraceID().String(); got != id {
		t.Fatalf("trace ID = %s", got)
	}
	finish(nil)
	if err := worker.telemetry.traceProvider.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := spans.GetSpans()
	if len(got) != 1 || got[0].Parent.IsValid() {
		t.Fatalf("invented remote parent: %#v", got)
	}
}
