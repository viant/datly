package transcribe

import "fmt"

type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// Position identifies a byte offset and its rune-aware source location.
type Position struct {
	Offset int
	Line   int
	Char   int
}

type Span struct {
	Start Position
	End   Position
}

// Diagnostic is transcribe compile feedback. It remains outside canonical
// spec because source spans and compiler hints are pipeline products.
type Diagnostic struct {
	Code     string
	Severity Severity
	Message  string
	Hint     string
	Path     string
	Span     Span
}

func (d *Diagnostic) Error() string {
	if d == nil {
		return ""
	}
	if d.Code == "" {
		return fmt.Sprintf("%s at line %d, char %d", d.Message, d.Span.Start.Line, d.Span.Start.Char)
	}
	return fmt.Sprintf("%s: %s at line %d, char %d", d.Code, d.Message, d.Span.Start.Line, d.Span.Start.Char)
}

func (d *Diagnostic) IsError() bool {
	return d != nil && d.Severity == SeverityError
}

// CompileError carries compiler diagnostics while preserving the parser error
// for errors.Is/errors.As classification.
type CompileError struct {
	Cause       error
	Diagnostics []*Diagnostic
}

func (e *CompileError) Error() string {
	if e == nil {
		return ""
	}
	if len(e.Diagnostics) > 0 {
		return e.Diagnostics[0].Error()
	}
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "transcribe compile failed"
}

func (e *CompileError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
