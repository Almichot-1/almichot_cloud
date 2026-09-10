package observability

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	TracerName        = "github.com/nebula/nebula"
	HeaderTraceParent = "traceparent"
	HeaderTraceID     = "X-Trace-ID"
	HeaderSpanID      = "X-Span-ID"
)

// SpanRecord captures span details for in-memory inspection and validation.
type SpanRecord struct {
	Name       string
	TraceID    string
	SpanID     string
	ParentSpan string
	Attributes map[string]string
}

// MemorySpanRecorder records spans in memory for end-to-end tracing assertions (§22).
type MemorySpanRecorder struct {
	mu    sync.RWMutex
	spans []SpanRecord
}

func NewMemorySpanRecorder() *MemorySpanRecorder {
	return &MemorySpanRecorder{
		spans: make([]SpanRecord, 0),
	}
}

func (r *MemorySpanRecorder) Record(span SpanRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, span)
}

func (r *MemorySpanRecorder) GetSpans() []SpanRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]SpanRecord, len(r.spans))
	copy(out, r.spans)
	return out
}

func (r *MemorySpanRecorder) FindByTraceID(traceID string) []SpanRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var matches []SpanRecord
	for _, s := range r.spans {
		if s.TraceID == traceID {
			matches = append(matches, s)
		}
	}
	return matches
}

func (r *MemorySpanRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = make([]SpanRecord, 0)
}

var (
	GlobalSpanRecorder = NewMemorySpanRecorder()
	tracer             trace.Tracer
	tracerInitOnce     sync.Once
	propagator         = propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
)

// InitTracer initializes OpenTelemetry tracer with an in-memory test exporter.
func InitTracer(serviceName string) trace.Tracer {
	tracerInitOnce.Do(func() {
		res, _ := resource.Merge(
			resource.Default(),
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(serviceName),
			),
		)
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
		)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagator)
		tracer = tp.Tracer(TracerName)
	})
	return tracer
}

func getTracer() trace.Tracer {
	return InitTracer("nebula-control-plane")
}

// StartSpan creates a new OpenTelemetry span and records it into GlobalSpanRecorder.
func StartSpan(ctx context.Context, spanName string) (context.Context, trace.Span) {
	tr := getTracer()
	newCtx, span := tr.Start(ctx, spanName)

	// Preserve existing trace ID from context if present
	traceIDStr := TraceIDFromContext(ctx)
	sc := span.SpanContext()

	if traceIDStr == "" {
		if sc.IsValid() && sc.TraceID().String() != "00000000000000000000000000000000" {
			traceIDStr = sc.TraceID().String()
		} else {
			traceIDStr = "trace-" + strings.ReplaceAll(uuid.New().String(), "-", "")
		}
	}

	spanIDStr := ""
	if sc.IsValid() && sc.SpanID().String() != "0000000000000000" {
		spanIDStr = sc.SpanID().String()
	} else {
		spanIDStr = "span-" + strings.ReplaceAll(uuid.New().String()[:16], "-", "")
	}

	parentID := ""
	if parentSpan := trace.SpanFromContext(ctx); parentSpan != nil && parentSpan.SpanContext().IsValid() {
		parentID = parentSpan.SpanContext().SpanID().String()
	}

	newCtx = ContextWithTraceID(newCtx, traceIDStr)
	newCtx = context.WithValue(newCtx, contextKeySpanID, spanIDStr)

	GlobalSpanRecorder.Record(SpanRecord{
		Name:       spanName,
		TraceID:    traceIDStr,
		SpanID:     spanIDStr,
		ParentSpan: parentID,
		Attributes: make(map[string]string),
	})

	return newCtx, span
}

// TraceIDFromContext extracts trace ID string from context.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyTraceID).(string); ok && v != "" {
		return v
	}
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() && sc.TraceID().String() != "00000000000000000000000000000000" {
		return sc.TraceID().String()
	}
	return ""
}

// SpanIDFromContext extracts span ID string from context.
func SpanIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return sc.SpanID().String()
	}
	if v, ok := ctx.Value(contextKeySpanID).(string); ok && v != "" {
		return v
	}
	return ""
}

type contextKey string

const (
	contextKeyTraceID contextKey = "nebula-trace-id"
	contextKeySpanID  contextKey = "nebula-span-id"
)

// ContextWithTraceID forces a specific trace ID into context.
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, contextKeyTraceID, traceID)
}

// InjectHTTPHeaders injects trace context into HTTP headers (W3C traceparent and X-Trace-ID).
func InjectHTTPHeaders(ctx context.Context, h http.Header) {
	propagator.Inject(ctx, propagation.HeaderCarrier(h))
	tid := TraceIDFromContext(ctx)
	if tid != "" {
		h.Set(HeaderTraceID, tid)
	}
}

// ExtractHTTPHeaders extracts trace context from HTTP headers.
func ExtractHTTPHeaders(ctx context.Context, h http.Header) context.Context {
	extracted := propagator.Extract(ctx, propagation.HeaderCarrier(h))
	if tid := h.Get(HeaderTraceID); tid != "" && TraceIDFromContext(extracted) == "" {
		extracted = ContextWithTraceID(extracted, tid)
	}
	return extracted
}

// LoggerWithTrace enriches a zerolog logger with trace_id and span_id from context.
func LoggerWithTrace(ctx context.Context, log zerolog.Logger) zerolog.Logger {
	tid := TraceIDFromContext(ctx)
	sid := SpanIDFromContext(ctx)
	l := log
	if tid != "" {
		l = l.With().Str("trace_id", tid).Logger()
	}
	if sid != "" {
		l = l.With().Str("span_id", sid).Logger()
	}
	return l
}

// EnsureTraceID ensures a valid trace ID exists in context, generating one if absent.
func EnsureTraceID(ctx context.Context) (context.Context, string) {
	tid := TraceIDFromContext(ctx)
	if tid == "" {
		tid = fmt.Sprintf("trace-%s", strings.ReplaceAll(uuid.New().String(), "-", ""))
		ctx = ContextWithTraceID(ctx, tid)
	}
	return ctx, tid
}
