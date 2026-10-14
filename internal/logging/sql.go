package logging

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/toolbox"
	"github.com/viant/xdatly/response"
)

func Configured(o owner) bool { return o != nil && o.loggingSink() != nil }

// LogSQL emits compatibility diagnostics under the sink's SQL policy.
// The raw cache evidence remains query-local and is never added to public metrics.
func LogSQL(o owner, traceID, view string, execution *response.SQLExecution, stats *cache.Stats) bool {
	if !Configured(o) {
		return false
	}
	o.loggingSink().sqlRead(traceID, view, execution, stats)
	return true
}

func (s *Sink) sqlRead(traceID, view string, e *response.SQLExecution, stats *cache.Stats) {
	if s == nil || e == nil {
		return
	}
	defer func() {
		if recover() != nil {
			s.diagnostic("[LOG-FORMAT-PANIC]")
		}
	}()
	if traceID == "" {
		traceID = "unknown"
	}
	traceID, view = SafeText(traceID), SafeText(view)
	context := ""
	if e.ID != "" {
		context += " executionId=" + SafeText(e.ID)
	}
	if e.ParentID != "" {
		context += " parentId=" + SafeText(e.ParentID)
	}
	timing := ""
	if !e.StartTime.IsZero() && !e.EndTime.IsZero() {
		timing = " elapsed=" + e.EndTime.Sub(e.StartTime).String()
	}
	errorCode := 0
	if stats != nil {
		errorCode = int(stats.ErrorCode)
	} else if e.CacheStats != nil {
		c := e.CacheStats
		errorCode = c.ErrorCode
		stats = &cache.Stats{Type: cache.Type(c.Type), RecordsCounter: c.RecordsCounter, Namespace: c.Namespace, Dataset: c.Dataset, FoundWarmup: c.FoundWarmup, FoundLazy: c.FoundLazy, ErrorType: c.ErrorType}
	}
	var records strings.Builder
	cacheContext := ""
	if stats != nil {
		source := cacheSource(stats)
		if errorCode != 0 {
			source = "error"
		}
		cacheContext = fmt.Sprintf(" source=%s type=%s found_warmup=%t found_lazy=%t records=%d namespace=%s set=%s cache_error_type=%q cache_error_code=%d", source, SafeText(string(stats.Type)), stats.FoundWarmup, stats.FoundLazy, stats.RecordsCounter, SafeText(stats.Namespace), SafeText(stats.Dataset), SafeText(stats.ErrorType), errorCode)
		level := "INFO"
		if stats.ErrorType != "" || errorCode != 0 {
			level = "WARN"
		}
		fmt.Fprintf(&records, "[%s] datly cache read reqTraceId=%s view=%s%s%s rows=%d%s\n", level, traceID, view, context, cacheContext, e.Rows, timing)
	}
	if e.Error != "" {
		sql := ""
		if s.sql {
			// Parameterized text only: positional values have no sensitivity labels.
			sql = fmt.Sprintf(" sql=%q", SafeText(e.SQL))
		}
		fmt.Fprintf(&records, "[ERROR] datly sql reqTraceId=%s view=%s error=%q%s rows=%d%s%s%s\n", traceID, view, FailureReason(e.Error, e.SQL), context, e.Rows, timing, cacheContext, sql)
	}
	if records.Len() == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.output != nil {
		_, _ = s.output.Write([]byte(records.String()))
	}
}

func cacheSource(s *cache.Stats) string {
	if s.ErrorType != "" {
		return "error"
	}
	switch s.Type {
	case cache.TypeReadMulti:
		return "warmup"
	case cache.TypeReadSingle:
		return "lazy"
	case cache.TypeWrite:
		return "miss_write"
	}
	if s.FoundWarmup {
		return "warmup"
	}
	if s.FoundLazy {
		return "lazy"
	}
	return "miss"
}

func normalizeDatabaseError(message string) string {
	if i := strings.LastIndex(message, ", due to "); i >= 0 {
		return strings.TrimSpace(message[i+len(", due to "):])
	}
	if i := strings.LastIndex(message, " due to "); i >= 0 {
		return strings.TrimSpace(message[i+len(" due to "):])
	}
	if i := strings.LastIndex(message, " failed to run query: "); i >= 0 {
		return strings.TrimSpace(message[:i])
	}
	if strings.HasPrefix(message, "failed to run query: ") {
		return "failed to run query"
	}
	return message
}

// expandDiagnosticSQL preserves the legacy display formatting; its result is
// never executed. An untyped nil is checked before reflection so diagnostics
// cannot panic on a valid NULL bind argument.
func expandDiagnosticSQL(sql string, args []any) string {
	for _, arg := range args {
		if arg != nil && reflect.TypeOf(arg).Kind() == reflect.Ptr {
			value := reflect.ValueOf(arg)
			if !value.IsNil() {
				arg = value.Elem().Interface()
			} else {
				arg = reflect.New(value.Type().Elem()).Elem().Interface()
			}
		}
		value := " NULL "
		if arg != nil {
			switch actual := arg.(type) {
			case int:
				value = strconv.Itoa(actual)
			case int64:
				value = strconv.Itoa(int(actual))
			case float64:
				value = strconv.FormatFloat(actual, 'f', 5, 32)
			case bool:
				value = strconv.FormatBool(actual)
			case time.Time:
				value = actual.Format(time.DateTime)
			case *time.Time:
				if actual != nil {
					value = actual.Format(time.DateTime)
				}
			default:
				value = "'" + toolbox.AsString(arg) + "'"
			}
		}
		sql = strings.Replace(sql, "?", value, 1)
	}
	return sql
}
