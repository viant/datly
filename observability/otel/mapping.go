package otel

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/viant/xdatly/response"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type idKey struct{}
type mappedIDs struct {
	trace trace.TraceID
	span  trace.SpanID
}

func (mappedIDs) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	ids := ctx.Value(idKey{}).(mappedIDs)
	return ids.trace, ids.span
}
func (mappedIDs) NewSpanID(ctx context.Context, _ trace.TraceID) trace.SpanID {
	return ctx.Value(idKey{}).(mappedIDs).span
}
func (mappedIDs) traceID(id string) trace.TraceID {
	if parsed, err := trace.TraceIDFromHex(strings.ReplaceAll(id, "-", "")); err == nil {
		return parsed
	}
	sum := sha256.Sum256([]byte("datly.trace:" + id))
	return trace.TraceID(sum[:16])
}
func (mappedIDs) spanID(traceID trace.TraceID, id string) trace.SpanID {
	if parsed, err := trace.SpanIDFromHex(id); err == nil {
		return parsed
	}
	sum := sha256.Sum256([]byte("datly.span:" + traceID.String() + ":" + id))
	return trace.SpanID(sum[:8])
}

type spanBatch struct{ spans []sdktrace.ReadOnlySpan }

func (*spanBatch) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (b *spanBatch) OnEnd(s sdktrace.ReadOnlySpan)                 { b.spans = append(b.spans, s) }
func (*spanBatch) Shutdown(context.Context) error                  { return nil }
func (*spanBatch) ForceFlush(context.Context) error                { return nil }

type mapper struct {
	tracer     trace.Tracer
	batch      *spanBatch
	includeSQL bool
}
type spanRecord struct {
	id, parent, name string
	start, end       time.Time
	failed           bool
	kind             trace.SpanKind
	attrs            []attribute.KeyValue
	links            []trace.Link
	unset            bool
}

func (m *mapper) convert(c Completion) ([]sdktrace.ReadOnlySpan, error) {
	m.batch.spans = nil
	native := c.Context
	if native == nil {
		return nil, fmt.Errorf("completed context is required")
	}
	traceName := native.TraceID
	rootID := ""
	hasNativeRoot := false
	var parent string
	rootKind := trace.SpanKindInternal
	start := native.StartTime
	if native.Trace != nil {
		if traceName == "" {
			traceName = native.Trace.TraceID
		}
		if len(native.Trace.Spans) > 0 && native.Trace.Spans[0] != nil {
			root := native.Trace.Spans[0]
			hasNativeRoot = true
			rootKind = m.nativeKind(root.Kind)
			c.Failed = c.Failed || root.Status.Code == "Error"
			rootID = root.SpanID
			start = root.StartTime
			if !root.EndTime.IsZero() {
				c.End = root.EndTime
			}
			if root.ParentSpanID != nil {
				parent = *root.ParentSpanID
			}
		}
	}
	if !hasNativeRoot {
		rootID = uuid.NewString()
	}
	if traceName == "" {
		traceName = uuid.NewString()
	}
	ids := mappedIDs{}
	traceID := ids.traceID(traceName)
	records := []spanRecord{{id: rootID, parent: parent, name: "datly request", start: start, end: c.End, failed: c.Failed, kind: rootKind}}
	if native.Method != "" {
		records[0].attrs = append(records[0].attrs, attribute.String("datly.method", native.Method))
	}
	if native.StatusCode != 0 {
		records[0].attrs = append(records[0].attrs, attribute.Int("datly.status_code", native.StatusCode))
	}
	for _, link := range c.Links {
		tid, err := trace.TraceIDFromHex(link.TraceID)
		if err != nil {
			if _, parseErr := uuid.Parse(link.TraceID); parseErr != nil {
				return nil, fmt.Errorf("invalid authoritative link trace ID")
			}
			tid = ids.traceID(link.TraceID)
		}
		sid, err := trace.SpanIDFromHex(link.SpanID)
		if err != nil {
			if _, parseErr := uuid.Parse(link.SpanID); parseErr != nil {
				return nil, fmt.Errorf("invalid authoritative link span ID")
			}
			sid = ids.spanID(tid, link.SpanID)
		}
		records[0].links = append(records[0].links, trace.Link{SpanContext: trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true})})
	}
	if native.Trace != nil {
		for index, span := range native.Trace.Spans {
			if index == 0 || span == nil {
				continue
			}
			parent := ""
			if span.ParentSpanID != nil {
				parent = *span.ParentSpanID
			}
			kind := m.nativeKind(span.Kind)
			records = append(records, spanRecord{id: span.SpanID, parent: parent, name: span.Name, kind: kind, start: span.StartTime, end: span.EndTime, failed: span.Status.Code == "Error", unset: span.Status.Code != "OK" && span.Status.Code != "Error"})
		}
	}
	for _, metric := range native.Metrics {
		if metric == nil {
			continue
		}
		projection := *metric
		// Original typed DML metrics have no ID or SQL executions. Assign an
		// export-only identity in the worker; never mutate native capture.
		generated := projection.ID == "" && len(projection.Executions) == 0 &&
			(projection.Type == "INSERT" || projection.Type == "UPDATE" || projection.Type == "DELETE")
		if generated {
			projection.ID = uuid.NewString()
		}
		projection.Executions = nil
		for _, execution := range metric.Executions {
			if execution != nil {
				projection.Executions = append(projection.Executions, execution)
			}
		}
		nativeSpans := response.Metrics{&projection}.ToSpans(&rootID)
		records = append(records, spanRecord{id: projection.ID, parent: rootID, name: nativeSpans[0].Name, kind: trace.SpanKindClient, start: metric.StartTime, end: metric.EndTime, failed: metric.Error != "", attrs: []attribute.KeyValue{attribute.String("datly.view", metric.View), attribute.String("db.operation.name", metric.Type), attribute.Int("datly.rows", metric.Rows), attribute.Int("datly.elapsed_ms", metric.ElapsedMs)}})
		if generated {
			records[len(records)-1].attrs = append(records[len(records)-1].attrs, attribute.Bool("datly.span.id.generated", true))
		}
		for index, e := range projection.Executions {
			if e == nil {
				continue
			}
			parent := e.ParentID
			if parent == "" {
				parent = projection.ID
			}
			r := spanRecord{id: nativeSpans[index+1].SpanID, parent: parent, name: nativeSpans[index+1].Name, kind: trace.SpanKindClient, start: e.StartTime, end: e.EndTime, failed: e.Error != "", attrs: []attribute.KeyValue{attribute.String("datly.view", metric.View), attribute.Int("db.response.returned_rows", e.Rows)}}
			if m.includeSQL && e.SQL != "" {
				r.attrs = append(r.attrs, attribute.String("db.query.text", e.SQL))
			}
			if s := e.CacheStats; s != nil {
				r.attrs = append(r.attrs, attribute.String("cache.type", s.Type), attribute.Int("cache.records", s.RecordsCounter), attribute.Bool("cache.warmup", s.FoundWarmup), attribute.Bool("cache.lazy", s.FoundLazy), attribute.Bool("cache.error", s.ErrorType != ""), attribute.Int("cache.error_code", s.ErrorCode))
				if s.ExpiryTime != nil {
					r.attrs = append(r.attrs, attribute.Int64("cache.expiry_unix_nano", s.ExpiryTime.UnixNano()))
				}
			}
			records = append(records, r)
		}
	}
	seen := map[string]bool{}
	mapped := map[trace.SpanID]string{}
	for _, r := range records {
		if r.id == "" || seen[r.id] {
			return nil, fmt.Errorf("empty or duplicate native span ID")
		}
		seen[r.id] = true
		if r.start.IsZero() || r.end.Before(r.start) {
			return nil, fmt.Errorf("invalid native timing interval")
		}
		id := ids.spanID(traceID, r.id)
		if prior, ok := mapped[id]; ok && prior != r.id {
			return nil, fmt.Errorf("native span ID mapping collision")
		}
		mapped[id] = r.id
	}
	parents := map[string]string{}
	for _, r := range records {
		parents[r.id] = r.parent
	}
	states := map[string]uint8{}
	var path []string
	for _, r := range records {
		path = path[:0]
		for id := r.id; id != "" && states[id] != 2; id = parents[id] {
			if states[id] == 1 {
				return nil, fmt.Errorf("cyclic native parent references")
			}
			states[id] = 1
			path = append(path, id)
		}
		for _, id := range path {
			states[id] = 2
		}
	}
	for _, r := range records {
		if r.parent == r.id {
			return nil, fmt.Errorf("self-parented native span")
		}
		ctx := context.Background()
		if r.parent != "" {
			ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: ids.spanID(traceID, r.parent), TraceFlags: trace.FlagsSampled, Remote: !seen[r.parent]}))
			if !seen[r.parent] {
				r.attrs = append(r.attrs, attribute.Bool("datly.parent.unresolved", true))
			}
		}
		filtered := r.attrs[:0]
		for _, a := range r.attrs {
			if (a.Key == "datly.rows" || a.Key == "db.response.returned_rows") && a.Value.AsInt64() < 0 {
				filtered = append(filtered, attribute.Bool("datly.rows.known", false))
				continue
			}
			filtered = append(filtered, a)
		}
		r.attrs = filtered
		ctx = context.WithValue(ctx, idKey{}, mappedIDs{trace: traceID, span: ids.spanID(traceID, r.id)})
		_, span := m.tracer.Start(ctx, r.name, trace.WithTimestamp(r.start), trace.WithSpanKind(r.kind), trace.WithAttributes(r.attrs...), trace.WithLinks(r.links...))
		if r.failed {
			span.SetStatus(codes.Error, "operation failed")
		} else if !r.unset {
			span.SetStatus(codes.Ok, "")
		}
		span.End(trace.WithTimestamp(r.end))
	}
	return m.batch.spans, nil
}

func (*mapper) nativeKind(kind string) trace.SpanKind {
	switch strings.ToUpper(kind) {
	case "CLIENT":
		return trace.SpanKindClient
	case "SERVER":
		return trace.SpanKindServer
	case "PRODUCER":
		return trace.SpanKindProducer
	case "CONSUMER":
		return trace.SpanKindConsumer
	default:
		return trace.SpanKindInternal
	}
}
