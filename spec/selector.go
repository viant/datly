package spec

// FieldPath is a metadata path used by selector policy. It remains a string
// because paths are authored and serialized, while runtime field access is
// compiled separately.
type FieldPath string

// Selector is immutable per-view query-selector policy. Request-time selector
// values live in xdatly/state.Selector and must not be stored here.
type Selector struct {
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
}

// SQLMethod permits one named SQL function in client criteria. Argument types
// are canonical Go type expressions resolved when the reader is compiled.
type SQLMethod struct {
	Name string   `json:"name"`
	Args []string `json:"args,omitempty"`
}
