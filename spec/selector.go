package spec

import "fmt"

// FieldPath is a metadata path used by selector policy. It remains a string
// because paths are authored and serialized, while runtime field access is
// compiled separately.
type FieldPath string

// Selector is immutable per-view query-selector policy. Request-time selector
// values live in xdatly/state.Selector and must not be stored here.
type Selector struct {
	// FieldSelection controls whether request Fields project SQL or only output.
	// Columns remain trusted read controls in either mode.
	FieldSelection FieldSelectionMode `json:"fieldSelection,omitempty"`
	// Specified distinguishes authored permissions (including false) from
	// defaults inferred from query-selector bindings. Use SetPermission to author.
	Specified     []SelectorProperty   `json:"specified,omitempty"`
	Namespace     string               `json:"namespace,omitempty"`
	AllowFields   bool                 `json:"allowFields,omitempty"`
	AllowOrderBy  bool                 `json:"allowOrderBy,omitempty"`
	AllowCriteria bool                 `json:"allowCriteria,omitempty"`
	AllowLimit    bool                 `json:"allowLimit,omitempty"`
	AllowOffset   bool                 `json:"allowOffset,omitempty"`
	AllowPage     bool                 `json:"allowPage,omitempty"`
	DefaultOrder  string               `json:"defaultOrder,omitempty"`
	DefaultLimit  int                  `json:"defaultLimit,omitempty"`
	NoLimit       bool                 `json:"noLimit,omitempty"`
	Filterable    []FieldPath          `json:"filterable,omitempty"`
	SQLMethods    []SQLMethod          `json:"sqlMethods,omitempty"`
	Orderable     []FieldPath          `json:"orderable,omitempty"`
	OrderAliases  map[string]FieldPath `json:"orderAliases,omitempty"`

	// RequireLimitForOffset requires a positive explicit limit for an offset
	// when the request has no page selector.
	RequireLimitForOffset bool `json:"requireLimitForOffset,omitempty"`
}

// SQLMethod permits one named SQL function in client criteria. Argument types
// are canonical Go type expressions resolved when the reader is compiled.
type SQLMethod struct {
	Name string   `json:"name"`
	Args []string `json:"args,omitempty"`
}

// FieldSelectionMode keeps external response selection independent of data needed
// by row and relation lifecycle hooks. The zero value preserves query projection.
type FieldSelectionMode string

const (
	FieldSelectionQuery      FieldSelectionMode = "query"
	FieldSelectionOutputOnly FieldSelectionMode = "outputOnly"
)

func (m FieldSelectionMode) Validate() error {
	switch m {
	case "", FieldSelectionQuery, FieldSelectionOutputOnly:
		return nil
	default:
		return fmt.Errorf("unsupported selector field selection mode %q", m)
	}
}
