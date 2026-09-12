package agnt5

import (
	"context"
	"log/slog"
)

// NewSlogHandler forwards context-aware application records to the invoking
// worker's telemetry pipeline while preserving the supplied handler and its
// filtering. Pass an invocation Context (or a context derived from it) to
// InfoContext/ErrorContext. Records without invocation context stay local.
// Configure with slog.New(NewSlogHandler(handler)), or install it with
// slog.SetDefault before starting workers. No process-global logger is changed
// by the SDK.
func NewSlogHandler(next slog.Handler) slog.Handler {
	if next == nil {
		panic("agnt5: nil slog handler")
	}
	return &telemetrySlogHandler{next: next}
}

type telemetrySlogHandler struct {
	next   slog.Handler
	fields map[string]any
	group  string
}

func (h *telemetrySlogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *telemetrySlogHandler) Handle(ctx context.Context, record slog.Record) error {
	if ctx != nil {
		if scope, ok := ctx.Value(telemetryContextKey).(*invocationTelemetry); ok && scope.telemetry != nil {
			fields := h.copyFields()
			record.Attrs(func(attr slog.Attr) bool { addSlogAttr(fields, h.group, attr); return true })
			level := "INFO"
			switch {
			case record.Level >= slog.LevelError:
				level = "ERROR"
			case record.Level >= slog.LevelWarn:
				level = "WARN"
			case record.Level < slog.LevelInfo:
				level = "DEBUG"
			}
			scope.telemetry.emit(ctx, level, record.Message, scope.runID, fields)
		}
	}
	return h.next.Handle(ctx, record)
}

func (h *telemetrySlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := h.copyFields()
	for _, attr := range attrs {
		addSlogAttr(fields, h.group, attr)
	}
	return &telemetrySlogHandler{next: h.next.WithAttrs(attrs), fields: fields, group: h.group}
}

func (h *telemetrySlogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &telemetrySlogHandler{next: h.next.WithGroup(name), fields: h.fields, group: h.group + name + "."}
}

func (h *telemetrySlogHandler) copyFields() map[string]any {
	fields := make(map[string]any, len(h.fields))
	for key, value := range h.fields {
		fields[key] = value
	}
	return fields
}

func addSlogAttr(fields map[string]any, prefix string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			prefix += attr.Key + "."
		}
		for _, child := range attr.Value.Group() {
			addSlogAttr(fields, prefix, child)
		}
		return
	}
	fields[prefix+attr.Key] = attr.Value.Any()
}
