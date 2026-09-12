package agnt5

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type invocationTelemetry struct {
	telemetry    *telemetry
	invocationID string
	runID        string
}

func (w *Worker) startInvocationTelemetry(ctx context.Context, inv Invocation) (context.Context, func(error)) {
	if ctx == nil {
		ctx = context.Background()
	}
	t := w.currentTelemetry()
	if scope, ok := ctx.Value(telemetryContextKey).(*invocationTelemetry); ok && scope.telemetry == t && scope.invocationID == inv.ID {
		return ctx, func(error) {}
	}
	// Incoming job identity takes precedence over a worker startup span. Extract
	// against an empty span context so an inherited parent is not mistaken for
	// a valid incoming W3C parent; cancellation and other context values survive.
	incoming := propagation.TraceContext{}.Extract(trace.ContextWithSpanContext(ctx, trace.SpanContext{}), propagation.MapCarrier(inv.Metadata))
	if trace.SpanContextFromContext(incoming).IsValid() {
		ctx = incoming
	} else if id, err := trace.TraceIDFromHex(inv.Metadata["trace_id"]); err == nil {
		ctx = trace.ContextWithSpanContext(ctx, trace.SpanContext{})
		ctx = context.WithValue(ctx, telemetryTraceIDContextKey, id)
	}
	ctx = context.WithValue(ctx, telemetryContextKey, &invocationTelemetry{telemetry: t, invocationID: inv.ID, runID: runIDForTelemetry(inv)})
	if t == nil || t.tracer == nil {
		return ctx, func(error) {}
	}
	componentType := inv.ComponentType
	if componentType == "" {
		if component, ok := w.registry.Get(inv.ComponentName); ok {
			componentType = component.Type
		}
	}
	ctx, span := t.tracer.Start(ctx, string(componentType)+"."+inv.ComponentName,
		trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(t.identityAttributes...), trace.WithAttributes(
			attribute.String("agnt5.run.id", runIDForTelemetry(inv)), attribute.String("run_id", runIDForTelemetry(inv)),
			attribute.String("agnt5.invocation.id", inv.ID), attribute.String("agnt5.component.name", inv.ComponentName),
			attribute.String("agnt5.component.type", string(componentType)), attribute.Int("agnt5.attempt", inv.Attempt)))
	return ctx, func(err error) { finishTelemetrySpan(span, err) }
}

func (c *Context) startTelemetrySpan(name string) (*Context, func(error)) {
	if c == nil || c.telemetry == nil || c.telemetry.tracer == nil {
		return c, func(error) {}
	}
	ctx, span := c.telemetry.tracer.Start(c.Context, name, trace.WithAttributes(c.telemetry.identityAttributes...), trace.WithAttributes(attribute.String("agnt5.run.id", c.RunID())))
	child := c.withParentCorrelationID(c.parentCID)
	child.Context = ctx
	return child, func(err error) { finishTelemetrySpan(span, err) }
}

// finishTelemetryScope is deferred directly so nested spans record a panic
// before it continues to the existing invocation recovery boundary.
func finishTelemetryScope(finish func(error), err *error) {
	if recovered := recover(); recovered != nil {
		finish(fmt.Errorf("agnt5: panic: %v", recovered))
		panic(recovered)
	}
	finish(*err)
}

func finishTelemetrySpan(span trace.Span, err error) {
	defer span.End()
	var suspension *durableSleepSuspensionError
	if IsWaitingForUserInput(err) || errors.As(err, &suspension) {
		span.SetAttributes(attribute.Bool("agnt5.suspended", true))
	} else if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// Pull dispatch can carry a trace ID without a parent span. Preserve that
// trace's identity while starting a real root span, rather than inventing a
// remote parent that was never recorded.
type runtimeTraceIDGenerator struct{}

func (runtimeTraceIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	id, _ := ctx.Value(telemetryTraceIDContextKey).(trace.TraceID)
	if !id.IsValid() {
		_, _ = rand.Read(id[:])
	}
	return id, runtimeTraceIDGenerator{}.NewSpanID(ctx, id)
}

func (runtimeTraceIDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	var id trace.SpanID
	_, _ = rand.Read(id[:])
	return id
}
