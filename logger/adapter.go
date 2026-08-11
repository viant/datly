package logger

import (
	"context"
	"log/slog"

	"github.com/viant/datly/internal/contextinfo"
	"github.com/viant/datly/internal/requesttrace"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/debug"
	"strings"
	"time"
)

type (
	Adapters     []*Adapter
	AdapterIndex map[string]*Adapter

	Adapter struct {
		shared.Reference
		Name string

		readTime          ReadTime
		readingData       ReadingData
		objectReconciling ObjectReconciling
		columnsDetection  ColumnsDetection
		log               Log
	}
)

func (i AdapterIndex) Lookup(name string) (*Adapter, bool) {
	adapter, ok := i[name]
	return adapter, ok
}

func (i AdapterIndex) Register(adapter *Adapter) {
	i[adapter.Name] = adapter
}

func (a Adapters) Index() AdapterIndex {
	result := AdapterIndex{}
	for i := range a {
		result[a[i].Name] = a[i]
	}

	return result
}

func (l *Adapter) ColumnsDetection(sql, source string) {
	if l.columnsDetection == nil {
		return
	}

	l.columnsDetection(sql, source)
}

func (l *Adapter) ObjectReconciling(dst, item, parent interface{}, index int) {
	return
}

func (l *Adapter) ReadingData(duration time.Duration, SQL string, read int, params []interface{}, err error) {
	if l.readingData == nil {
		return
	}

	l.readingData(duration, SQL, read, params, err)
}

func (l *Adapter) ReadTime(viewName string, start, end *time.Time, err error) {
	if l.readTime == nil {
		return
	}

	l.readTime(viewName, start, end, err)
}

func (l *Adapter) Log(message string, args ...interface{}) {
	if l.log == nil {
		return
	}

	l.log(message, args...)
}

func (l *Adapter) Inherit(adapter *Adapter) {
	l.readTime = adapter.readTime
	l.readingData = adapter.readingData
	l.objectReconciling = adapter.objectReconciling
	l.columnsDetection = adapter.columnsDetection
	l.log = adapter.log
}

func (l *Adapter) LogDatabaseErr(ctx context.Context, view string, SQL string, err error, args ...interface{}) {
	SQL = shared.ExpandSQL(SQL, args)
	details := contextinfo.Snapshot(ctx)
	slog.LogAttrs(ctx, slog.LevelError, "datly sql",
		slog.String("reqTraceId", reqTraceID(ctx)),
		slog.String("view", view),
		slog.String("error", normalizeDatabaseError(err)),
		slog.String("ctxErr", details.Err),
		slog.String("cause", details.Cause),
		slog.Bool("hasDeadline", details.HasDeadline),
		slog.String("deadline", details.Deadline),
		slog.String("remaining", details.Remaining),
		slog.String("sql", strings.ReplaceAll(SQL, "\n", "\\n")),
		slog.Any("params", args))
}

func reqTraceID(ctx context.Context) string {
	if traceID := requesttrace.Current(ctx); traceID != "" {
		return traceID
	}
	return "unknown"
}

func normalizeDatabaseError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if idx := strings.LastIndex(message, ", due to "); idx >= 0 {
		return strings.TrimSpace(message[idx+len(", due to "):])
	}
	if idx := strings.LastIndex(message, " due to "); idx >= 0 {
		return strings.TrimSpace(message[idx+len(" due to "):])
	}
	if idx := strings.LastIndex(message, " failed to run query: "); idx >= 0 {
		return strings.TrimSpace(message[:idx])
	}
	if strings.HasPrefix(message, "failed to run query: ") {
		return "failed to run query"
	}
	return message
}

func NewLogger(name string, logger Logger) *Adapter {
	if logger == nil {
		return &Adapter{
			Name: name,
		}
	}

	return &Adapter{
		Name:              name,
		Reference:         shared.Reference{},
		readTime:          logger.ViewReadTime(),
		readingData:       logger.ReadingData(),
		objectReconciling: logger.ObjectReconciling(),
		columnsDetection:  logger.ColumnsDetection(),
		log:               logger.Log(),
	}
}

func Default() *Adapter {
	if !debug.Enabled {
		return NewLogger("", nil)
	}
	return NewLogger("", &defaultLogger{})
}
