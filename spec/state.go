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

// WireSchema is discovery-only metadata for a public wire contract. Runtime
// binding continues to use the compiled source and destination types.
type WireSchema struct {
	Type     string `json:"type,omitempty"`
	Format   string `json:"format,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
}

func (s *WireSchema) Clone() *WireSchema {
	if s == nil {
		return nil
	}
	result := *s
	return &result
}

type Parameter struct {
	Name           string          `json:"name,omitempty"`
	Declaration    DeclarationKind `json:"declaration,omitempty"`
	Source         BindSource      `json:"source,omitempty"`
	TypeExpr       string          `json:"typeExpr,omitempty"`
	OutputTypeExpr string          `json:"outputTypeExpr,omitempty"`
	// DeclarationSQL preserves the `/* ... */` SQL/comment body attached to a
	// declaration as an authored input to later transcribe stages.
	DeclarationSQL    string                 `json:"declarationSQL,omitempty"`
	Tag               string                 `json:"tag,omitempty"`
	Cardinality       string                 `json:"cardinality,omitempty"`
	Required          *bool                  `json:"required,omitempty"`
	MinAllowedRecords *int                   `json:"minAllowedRecords,omitempty"`
	MaxAllowedRecords *int                   `json:"maxAllowedRecords,omitempty"`
	ExpectedReturned  *int                   `json:"expectedReturned,omitempty"`
	Cacheable         *bool                  `json:"cacheable,omitempty"`
	When              string                 `json:"when,omitempty"`
	Scope             string                 `json:"scope,omitempty"`
	With              string                 `json:"with,omitempty"`
	Activation        *RouteActivation       `json:"activation,omitempty"`
	MCP               *bool                  `json:"mcp,omitempty"`
	PathMCP           *bool                  `json:"pathMcp,omitempty"`
	ResourceRef       string                 `json:"resourceRef,omitempty"`
	Value             *string                `json:"value,omitempty"`
	Async             bool                   `json:"async,omitempty"`
	ErrorStatusCode   int                    `json:"errorStatusCode,omitempty"`
	ErrorMessage      string                 `json:"errorMessage,omitempty"`
	Description       string                 `json:"description,omitempty"`
	Example           string                 `json:"example,omitempty"`
	EmitOutput        bool                   `json:"emitOutput,omitempty"`
	Predicates        []*Predicate           `json:"predicates,omitempty"`
	Codec             *Codec                 `json:"codec,omitempty"`
	QuerySelector     *QuerySelectorBinding  `json:"querySelector,omitempty"`
	FormatSelector    bool                   `json:"formatSelector,omitempty"`
	WireSchema        *WireSchema            `json:"wireSchema,omitempty"`
	WireSchemas       map[string]*WireSchema `json:"wireSchemas,omitempty"`
	Raw               string                 `json:"raw,omitempty"`
}
