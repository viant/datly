package spec

import "strings"

type DeclarationKind string

const (
	DeclarationKindDefine DeclarationKind = "define"
	DeclarationKindSet    DeclarationKind = "set"
)

type BindSource struct {
	Kind string `json:"kind,omitempty"`
	Name string `json:"name,omitempty"`
}

// RouteActivation limits a parameter to an exact authored path or the original
// Datly relative placeholder suffix (for example /{id}).
type RouteActivation struct {
	URI string `json:"uri"`
}

func (a *RouteActivation) Matches(path string) bool {
	if a == nil {
		return true
	}
	uri := strings.TrimSpace(a.URI)
	path = strings.TrimSpace(path)
	if strings.HasPrefix(uri, "/{") {
		return strings.HasSuffix(path, uri)
	}
	return path == uri
}

type Predicate struct {
	Group           int      `json:"group,omitempty"`
	Name            string   `json:"name,omitempty"`
	Args            []string `json:"args,omitempty"`
	ApplyWhenAbsent bool     `json:"applyWhenAbsent,omitempty"`
}

type Codec struct {
	Body       string   `json:"body,omitempty"`
	Args       []string `json:"args,omitempty"`
	OutputType string   `json:"outputType,omitempty"`
}

type Parameter struct {
	Name           string          `json:"name,omitempty"`
	Declaration    DeclarationKind `json:"declaration,omitempty"`
	Source         BindSource      `json:"source,omitempty"`
	TypeExpr       string          `json:"typeExpr,omitempty"`
	OutputTypeExpr string          `json:"outputTypeExpr,omitempty"`
	// DeclarationSQL preserves the `/* ... */` SQL/comment body attached to a
	// declaration as an authored input to later transcribe stages.
	DeclarationSQL    string                `json:"declarationSQL,omitempty"`
	Tag               string                `json:"tag,omitempty"`
	Cardinality       string                `json:"cardinality,omitempty"`
	Required          *bool                 `json:"required,omitempty"`
	MinAllowedRecords *int                  `json:"minAllowedRecords,omitempty"`
	MaxAllowedRecords *int                  `json:"maxAllowedRecords,omitempty"`
	ExpectedReturned  *int                  `json:"expectedReturned,omitempty"`
	Cacheable         *bool                 `json:"cacheable,omitempty"`
	When              string                `json:"when,omitempty"`
	Scope             string                `json:"scope,omitempty"`
	With              string                `json:"with,omitempty"`
	Activation        *RouteActivation      `json:"activation,omitempty"`
	MCP               *bool                 `json:"mcp,omitempty"`
	PathMCP           *bool                 `json:"pathMcp,omitempty"`
	ResourceRef       string                `json:"resourceRef,omitempty"`
	Value             *string               `json:"value,omitempty"`
	Async             bool                  `json:"async,omitempty"`
	ErrorStatusCode   int                   `json:"errorStatusCode,omitempty"`
	ErrorMessage      string                `json:"errorMessage,omitempty"`
	Description       string                `json:"description,omitempty"`
	Example           string                `json:"example,omitempty"`
	EmitOutput        bool                  `json:"emitOutput,omitempty"`
	Predicates        []*Predicate          `json:"predicates,omitempty"`
	Codec             *Codec                `json:"codec,omitempty"`
	QuerySelector     *QuerySelectorBinding `json:"querySelector,omitempty"`
	Raw               string                `json:"raw,omitempty"`
}
