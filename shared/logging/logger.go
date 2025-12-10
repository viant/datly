package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	regexp "regexp"
	"runtime"
	strings "strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/logger"
)

const (
	ReqId                = "RequestId"
	OpenTelemetryTraceId = "OpenTelemetryTraceId"
	DEBUG                = "DEBUG"
	INFO                 = "INFO"
	WARN                 = "WARN"
	ERROR                = "ERROR"
	UNKNOWN              = "UNKNOWN" // Indicate other environment
)

type slogger struct {
	logger *slog.Logger
	level  slog.Level
}

// Init creates an ISLogger instance, a structured logger using the JSON Handler.
// Creating this logger sets this as the default logger, so any logging after this
// which goes through the standard logging package will also produce JSON structured
// logs.
func New(level string, dest io.Writer) logger.Logger {
	if dest == nil {
		dest = os.Stdout
	}

	logLevel := slog.LevelInfo
	switch strings.ToUpper(level) {
	case DEBUG:
		logLevel = slog.LevelDebug
	case WARN:
		logLevel = slog.LevelWarn
	case ERROR:
		logLevel = slog.LevelError
	}

	handler := slog.NewJSONHandler(dest, &slog.HandlerOptions{
		AddSource: false,
		Level:     logLevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Rename the time key to "timestamp"
			if a.Key == slog.TimeKey {
				a.Key = "timestamp"
			}
			return a
		},
	})
	sl := slog.New(handler)
	slog.SetDefault(sl)
	logger := &slogger{sl, logLevel}

	return logger
}

func (s *slogger) IsDebugEnabled() bool {
	return s.level.Level() <= slog.LevelDebug
}

func (s *slogger) IsInfoEnabled() bool {
	return s.level.Level() <= slog.LevelInfo
}

func (s *slogger) IsWarnEnabled() bool {
	return s.level.Level() <= slog.LevelWarn
}

func (s *slogger) IsErrorEnabled() bool {
	return s.level.Level() <= slog.LevelError
}

// getCallerInfo uses runtime to get the caller's program counter
// and extract info from the stack frame to get the function name, etc.
func (s *slogger) getCallerInfo() []any {
	callers := make([]uintptr, 1)
	count := runtime.Callers(3, callers[:]) // skip to actual caller
	if count == 0 {
		slog.Warn("getCallerInfo: no frames, exiting")
		return nil
	}

	frames := runtime.CallersFrames(callers)
	var frame runtime.Frame
	var more bool
	for {
		frame, more = frames.Next()
		if !more {
			break
		}
	}

	attr := []any{
		"function", frame.Function, "file", frame.File, "line", frame.Line,
	}

	return attr
}

// getContextValues retrieves "known" logging values from the Context.
// These values can be added to the Context using the provided utility functions.
func (s *slogger) getContextValues(ctx context.Context) []any {
	var values []any
	if ctx == nil {
		slog.Warn("getContextValues: ctx is nil")
		return nil
	}

	openTelemetryTraceId := ctx.Value(OpenTelemetryTraceId)
	if openTelemetryTraceId != nil {
		values = append(values, "OpenTelemetryTraceId", openTelemetryTraceId)
	}

	execContext := exec.GetContext(ctx)
	if execContext != nil {
		traceId := "unknown"

		// ideally TraceID and Trace.TraceID should be the same
		// but xdatly/handler/exec.(*Context).setHeader TraceID first
		// with the value of adp-request-id header
		if execContext.TraceID != "" {
			traceId = execContext.TraceID
		} else if execContext.Trace != nil {
			traceId = execContext.Trace.TraceID
		}
		values = append(values, "reqTraceId", traceId)
	}

	return values
}

// Info wraps a call to slog.Info, inserting details for the calling function.
func (s *slogger) Info(msg string, args ...any) {
	if !s.IsInfoEnabled() {
		return
	}
	caller := s.getCallerInfo()
	caller = append(caller, args...)
	s.logger.Info(msg, caller...)
}

// Debug wraps a call to slog.Debug, inserting details for the calling function.
func (s *slogger) Debug(msg string, args ...any) {
	if !s.IsDebugEnabled() {
		return
	}
	caller := s.getCallerInfo()
	caller = append(caller, args...)
	s.logger.Debug(msg, caller...)
}

// Warn wraps a call to slog.Warn, inserting details for the calling function.
func (s *slogger) Warn(msg string, args ...any) {
	if !s.IsWarnEnabled() {
		return
	}
	caller := s.getCallerInfo()
	caller = append(caller, args...)
	s.logger.Warn(msg, caller...)
}

// Error wraps a call to slog.Error, inserting details for the calling function.
func (s *slogger) Error(msg string, args ...any) {
	if !s.IsErrorEnabled() {
		return
	}
	caller := s.getCallerInfo()
	caller = append(caller, args...)
	s.logger.Error(msg, caller...)
}

// Infoc wraps a call to slog.Info, inserting details for the calling function,
// and retrieving known values from the context object.
func (s *slogger) Infoc(ctx context.Context, msg string, args ...any) {
	if !s.IsInfoEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, args...)
	s.logger.Info(msg, caller...)
}

func (s *slogger) Infos(ctx context.Context, msg string, attrs ...slog.Attr) {
	if !s.IsInfoEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, redactAttrs(attrs...)...)

	s.logger.Info(msg, caller...)
}

// Debugc wraps a call to slog.Debug, inserting details for the calling function,
// and retrieving known values from the context object.
func (s *slogger) Debugc(ctx context.Context, msg string, args ...any) {
	if !s.IsDebugEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, args...)
	s.logger.Debug(msg, caller...)
}

func (s *slogger) Debugs(ctx context.Context, msg string, attrs ...slog.Attr) {
	if !s.IsDebugEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, redactAttrs(attrs...)...)

	s.logger.Debug(msg, caller...)
}

// DebugJSONc wraps a call to slog.Debug, inserting details for the calling function,
// and retrieving known values from the context object.
func (s *slogger) DebugJSONc(ctx context.Context, msg string, obj any) {
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)

	// Initialize request and jsonData variables
	var request events.APIGatewayProxyRequest
	var jsonData []byte
	// Marshal the object to JSON string
	jsonString, _ := json.Marshal(obj)
	// Unmarshal JSON string to APIGatewayProxyRequest
	err := json.Unmarshal(jsonString, &request)
	if err != nil {
		return
	}

	// Check if the request has an HTTP method
	if len(request.HTTPMethod) > 0 {
		if request.MultiValueHeaders == nil {
			request.MultiValueHeaders = make(map[string][]string)
		}
		// Remove Authorization header
		request.MultiValueHeaders["Authorization"] = nil
		request.Headers["Authorization"] = ""
		// Marshal the modified request to JSON
		jsonData, _ = json.Marshal(request)
	} else {
		jsonData = jsonString
	}
	msg = fmt.Sprintf("%s %s", msg, string(jsonData))
	s.Debugc(ctx, msg, caller...)
}

// Warnc wraps a call to slog.Warn, inserting details for the calling function,
// and retrieving known values from the context object.
func (s *slogger) Warnc(ctx context.Context, msg string, args ...any) {
	if !s.IsWarnEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, args...)
	s.logger.Warn(msg, caller...)
}

func (s *slogger) Warns(ctx context.Context, msg string, attrs ...slog.Attr) {
	if !s.IsWarnEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, redactAttrs(attrs...)...)

	s.logger.Warn(msg, caller...)
}

// Errorc wraps a call to slog.Error, inserting details for the calling function,
// and retrieving known values from the context object.
func (s *slogger) Errorc(ctx context.Context, msg string, args ...any) {
	if !s.IsErrorEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, args...)
	s.logger.Error(msg, caller...)
}

func (s *slogger) Errors(ctx context.Context, msg string, attrs ...slog.Attr) {
	if !s.IsErrorEnabled() {
		return
	}
	caller := s.getCallerInfo()
	values := s.getContextValues(ctx)
	caller = append(caller, values...)
	caller = append(caller, redactAttrs(attrs...)...)

	s.logger.Error(msg, caller...)
}

// Helper to get platform from environment suffix
func getPlatformFromEnv(environment string) string {
	switch {
	case strings.Contains(environment, "dev"):
		return "development"
	case strings.Contains(environment, "stage"):
		return "stage"
	case strings.Contains(environment, "prod"):
		return "production"
	default:
		return UNKNOWN
	}
}

// redactAttrs applies redaction rules to slog.Attr list.
// Skip redactValue for primitive types to avoid unnecessary processing
// This avoids redundant type switch/marshalling cost in high-volume logging
func redactAttrs(attrs ...slog.Attr) []any {
	var result []any
	for _, attr := range attrs {
		if isSensitiveKey(attr.Key) {
			result = append(result, slog.String(attr.Key, "[REDACTED]"))
			continue
		}
		val := attr.Value.Any()
		switch val.(type) {
		case int, int64, float64, bool, nil:
			result = append(result, attr)
		default:
			redactedValue := slog.AnyValue(redactValue(val))
			result = append(result, slog.Attr{Key: attr.Key, Value: redactedValue})
		}
	}
	return result
}

// redactValue recursively redacts sensitive info in maps, slices, or structs.
func redactValue(value any) any {
	switch v := value.(type) {
	case string:
		// Redact sensitive information in strings
		return redactSensitiveInfo(v)
	case int, int64, float64, bool, nil:
		// Return primitive values directly (skip JSON marshalling)
		return v
	case map[string]any:
		// Redact value if key is sensitive (e.g., Authorization → [REDACTED])
		// Ensures map fields are redacted even if value doesn’t match regex
		for key, val := range v {
			if isSensitiveKey(key) {
				v[key] = "[REDACTED]"
			} else {
				v[key] = redactValue(val)
			}
		}
		return v
	case []any:
		// Recursively redact sensitive information in slices
		for i, val := range v {
			v[i] = redactValue(val)
		}
		return v
	default:
		// Only marshal/unmarshal if absolutely needed (structs, unknown).
		jsonData, err := json.Marshal(v) // Converts struct to map to enable nested field redaction.
		if err != nil {
			return v // If marshal fails, skip redaction
		}
		var unmarshaled any
		if err := json.Unmarshal(jsonData, &unmarshaled); err != nil {
			return v // If unmarshal fails, skip redaction
		}
		return redactValue(unmarshaled)
	}
}

// redactSensitiveInfo redacts known patterns in a string (e.g., tokens in URLs).
func redactSensitiveInfo(value string) string {
	sensitivePatterns := []*regexp.Regexp{
		// Redact key=value style
		regexp.MustCompile(`(?i)(X-Amz-Security-Token|X-Amz-Signature|X-Amz-Credential|Authorization|password|token|apiKey)=([^&\s]+)`),
		// Redact key: value or key value
		regexp.MustCompile(`(?i)(Authorization|password|token|apiKey)[\s:=]+([^&\s]+)`),
		// Redact URL with user:pass@host
		regexp.MustCompile(`(?i)https?://[^/]+:[^@]+@`),
	}

	redacted := value
	for _, pattern := range sensitivePatterns {
		redacted = pattern.ReplaceAllString(redacted, "$1=[REDACTED]")
	}
	return redacted
}

// isSensitiveKey returns true if the key is known to contain sensitive data.
func isSensitiveKey(key string) bool {
	sensitiveKeys := []string{
		"authorization", "token", "apikey", "password",
		"credential", "secret", "access_key", "secret_key",
	}
	key = strings.ToLower(key)
	for _, sk := range sensitiveKeys {
		if key == sk {
			return true
		}
	}
	return false
}
