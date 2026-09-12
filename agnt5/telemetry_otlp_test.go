package agnt5

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	pb "github.com/agnt5dev/sdk-go/internal/pb/api/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type localLogCollector struct {
	collectorlogs.UnimplementedLogsServiceServer
	received chan *collectorlogs.ExportLogsServiceRequest
}

func (c *localLogCollector) Export(_ context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	c.received <- req
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

type localTraceCollector struct {
	collectortrace.UnimplementedTraceServiceServer
	received chan *collectortrace.ExportTraceServiceRequest
}

func (c *localTraceCollector) Export(_ context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	c.received <- req
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

func TestTelemetryOTLPDispatchAndShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	logs := &localLogCollector{received: make(chan *collectorlogs.ExportLogsServiceRequest, 10)}
	traces := &localTraceCollector{received: make(chan *collectortrace.ExportTraceServiceRequest, 10)}
	collectorlogs.RegisterLogsServiceServer(server, logs)
	collectortrace.RegisterTraceServiceServer(server, traces)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	for _, signal := range []string{"", "_LOGS", "_TRACES"} {
		t.Setenv("OTEL_EXPORTER_OTLP"+signal+"_ENDPOINT", "http://"+listener.Addr().String())
		t.Setenv("OTEL_EXPORTER_OTLP"+signal+"_HEADERS", "")
	}
	worker := NewWorker("wire-test", WithWorkerID("worker-1"), WithWorkspaceID("12345678-1234-4234-8234-123456789abc"), WithProjectID("project-1"), WithDeploymentID("deployment-1"))
	worker.initializeTelemetry(context.Background())
	t.Cleanup(worker.shutdownTelemetry)
	logger := slog.New(NewSlogHandler(slog.NewTextHandler(io.Discard, nil)))
	if err := RegisterFunction(worker, "greet", func(ctx *Context, input string) (string, error) {
		logger.InfoContext(ctx, "application message")
		return input, nil
	}); err != nil {
		t.Fatal(err)
	}
	const traceID = "0123456789abcdef0123456789abcdef"
	messages := worker.dispatchServiceMessages(context.Background(), &pb.DispatchComponentRequest{
		InvocationId: "run-1", ComponentName: "greet", ComponentType: pb.ComponentType_COMPONENT_TYPE_FUNCTION,
		InputData: []byte(`"hello"`), Metadata: map[string]string{"traceparent": "00-" + traceID + "-0123456789abcdef-01"},
	})
	if len(messages) == 0 || !messages[len(messages)-1].GetFunctionResponse().GetSuccess() {
		t.Fatal("dispatch failed")
	}
	// Shutdown must drain both exporters without waiting for a batch timer.
	worker.shutdownTelemetry()
	// Exporters have returned from shutdown, so all acknowledged requests are
	// now buffered. Batch timers may have split records across several RPCs.
	logRequest := &collectorlogs.ExportLogsServiceRequest{}
	traceRequest := &collectortrace.ExportTraceServiceRequest{}
	for len(logs.received) > 0 {
		logRequest.ResourceLogs = append(logRequest.ResourceLogs, (<-logs.received).ResourceLogs...)
	}
	for len(traces.received) > 0 {
		traceRequest.ResourceSpans = append(traceRequest.ResourceSpans, (<-traces.received).ResourceSpans...)
	}
	spanCount := 0
	var spanID []byte
	for _, resource := range traceRequest.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				spanCount++
				spanID = span.SpanId
				attrs := make(map[string]string)
				for _, attr := range span.Attributes {
					attrs[attr.Key] = attr.Value.GetStringValue()
				}
				if attrs["agnt5.workspace.id"] != "12345678-1234-4234-8234-123456789abc" || attrs["agnt5.project.id"] != "project-1" || attrs["agnt5.deployment.id"] != "deployment-1" {
					t.Fatalf("wire span identity = %#v", attrs)
				}
			}
		}
	}
	if spanCount != 1 {
		t.Fatalf("dispatch produced %d spans, want 1", spanCount)
	}
	recordCount := 0
	for _, resource := range logRequest.ResourceLogs {
		attrs := map[string]string{}
		for _, attr := range resource.Resource.Attributes {
			attrs[attr.Key] = attr.Value.GetStringValue()
		}
		if attrs["agnt5.app_name"] != "wire-test" || attrs["agnt5.project.id"] != "project-1" || attrs["agnt5.deployment.id"] != "deployment-1" || attrs["agnt5.worker.id"] != "worker-1" {
			t.Fatalf("wire resource = %#v", attrs)
		}
		for _, scope := range resource.ScopeLogs {
			for _, record := range scope.LogRecords {
				recordCount++
				attrs := map[string]string{}
				for _, attr := range record.Attributes {
					attrs[attr.Key] = attr.Value.GetStringValue()
				}
				if attrs["log_source"] != "application" || attrs["agnt5.run.id"] != "run-1" || attrs["run_id"] != "run-1" || attrs["trace_id"] != traceID || string(record.SpanId) != string(spanID) {
					t.Fatalf("wire record correlation = %#v", attrs)
				}
			}
		}
	}
	if recordCount != 4 {
		t.Fatalf("wire logs = %d, want application plus three lifecycle lines", recordCount)
	}
}

func TestTelemetryTraceExporterRequiresEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://127.0.0.1:4317")
	worker := NewWorker("service")
	worker.initializeTelemetry(context.Background())
	t.Cleanup(worker.shutdownTelemetry)
	if worker.telemetry == nil || worker.telemetry.provider == nil {
		t.Fatal("existing log pipeline was disabled")
	}
	if worker.telemetry.traceProvider != nil || worker.telemetry.tracer != nil {
		t.Fatal("trace exporter enabled without a configured endpoint")
	}
}
