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

// LogSQL emits the original diagnostics independently of audit/trace SQL policy.
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
	var records strings.Builder
	if stats != nil {
		suffix := ""
		if stats.WarmupKey != "" {
			suffix += " warmup_key=" + stats.WarmupKey
		}
		if stats.MarkerKey != "" {
			suffix += " marker_key=" + stats.MarkerKey
		}
		fmt.Fprintf(&records, "[INFO] datly cache read reqTraceId=%s view=%s source=%s type=%s found_warmup=%t found_lazy=%t records=%d rows=%d namespace=%s set=%s elapsed=%s args=%v%s\n", traceID, view, cacheSource(stats), stats.Type, stats.FoundWarmup, stats.FoundLazy, stats.RecordsCounter, e.Rows, stats.Namespace, stats.Dataset, e.EndTime.Sub(e.StartTime), e.Args, suffix)
	}
	if e.Error != "" {
		expanded := expandDiagnosticSQL(e.SQL, e.Args)
		fmt.Fprintf(&records, "[ERROR] datly sql reqTraceId=%s view=%s error=%q sql=%q params=%v\n", traceID, view, normalizeDatabaseError(e.Error), strings.ReplaceAll(expanded, "\n", `\n`), e.Args)
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
