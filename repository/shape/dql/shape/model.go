package shape

import (
	"fmt"

	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/sqlparser/query"
)

// Severity represents diagnostic severity level.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// Position identifies a byte offset and human-readable line/character location.
type Position struct {
	Offset int
	Line   int
	Char   int
}

// Span captures the location range for one diagnostic.
type Span struct {
	Start Position
	End   Position
}

// Diagnostic represents one compile/parse issue with precise location.
type Diagnostic struct {
	Code     string
	Severity Severity
	Message  string
	Hint     string
	Span     Span
}

// Directives captures special #set(...) directives parsed from DQL.
type Directives struct {
	Meta              string
	DefaultConnector  string
	Cache             *CacheDirective
	MCP               *MCPDirective
	Route             *RouteDirective
	JSONMarshalType   string
	JSONUnmarshalType string
	XMLUnmarshalType  string
	Format            string
	DateFormat        string
	CaseFormat        string
}

type CacheDirective struct {
	Enabled bool
	TTL     string
}

type MCPDirective struct {
	Name            string
	Description     string
	DescriptionPath string
}

type RouteDirective struct {
	URI     string
	Methods []string
}

type Route struct {
	Name        string
	URI         string
	Method      string
	ViewRef     string
	Description string
}

type Resource struct {
	Views []*View
}

type View struct {
	Name         string
	Table        string
	Module       string
	ConnectorRef string
}

// Document represents parsed DQL model used by shape compiler and xgen.
type Document struct {
	Raw             string
	SQL             string
	Query           *query.Select
	TypeContext     *typectx.Context
	Directives      *Directives
	Routes          []*Route
	Resource        *Resource
	Root            map[string]any
	TypeResolutions []typectx.Resolution
	Diagnostics     []*Diagnostic
}

// Error returns a compact human-readable diagnostic string.
func (d *Diagnostic) Error() string {
	if d == nil {
		return ""
	}
	if d.Code == "" {
		return fmt.Sprintf("%s at line %d, char %d", d.Message, d.Span.Start.Line, d.Span.Start.Char)
	}
	return fmt.Sprintf("%s: %s at line %d, char %d", d.Code, d.Message, d.Span.Start.Line, d.Span.Start.Char)
}
