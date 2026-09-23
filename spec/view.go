package spec

import (
	"fmt"
	"strings"
)

type View struct {
	Key       Key    `json:"key"`
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	// Auxiliary is generation-only mutation intent. Reads and business data
	// remain part of the canonical view graph; runtime data.View does not own it.
	Auxiliary bool `json:"auxiliary,omitempty"`
	// InMemory marks a child populated by its parent hook. Source is retained
	// for column discovery only; runtime traverses the existing holder rows.
	InMemory bool `json:"inMemory,omitempty"`
	// TypeName and Dest retain authored package-generation choices. Runtime
	// view resolution deliberately ignores both fields.
	TypeName string `json:"typeName,omitempty"`
	Dest     string `json:"dest,omitempty"`
	// EntityHooks is generation-only metadata for the authored entity hook reference.
	EntityHooks           string         `json:"entityHooks,omitempty"`
	Cardinality           Cardinality    `json:"cardinality,omitempty"`
	AllowNulls            *bool          `json:"allowNulls,omitempty"`
	Groupable             *bool          `json:"groupable,omitempty"`
	Columns               []*Column      `json:"columns,omitempty"`
	Source                *ViewSource    `json:"source,omitempty"`
	Selector              *Selector      `json:"selector,omitempty"`
	Partitioning          *Partitioning  `json:"partitioning,omitempty"`
	SelfReference         *SelfReference `json:"selfReference,omitempty"`
	BatchSize             int            `json:"batchSize,omitempty"`
	BatchConcurrency      int            `json:"batchConcurrency,omitempty"`
	PublishParent         bool           `json:"publishParent,omitempty"`
	RelationalConcurrency int            `json:"relationalConcurrency,omitempty"`
	Relations             []*Relation    `json:"relations,omitempty"`
}

func (v *View) CanonicalName() string {
	if v == nil {
		return ""
	}
	if name := strings.TrimSpace(v.Name); name != "" {
		return name
	}
	return strings.TrimSpace(v.Key.Name)
}

// RuntimeSource excludes discovery-only SQL without mutating authored metadata.
func (v *View) RuntimeSource() *ViewSource {
	if v == nil {
		return nil
	}
	if !v.InMemory || v.Source == nil {
		return v.Source
	}
	result := v.Source.Clone()
	result.SQL, result.URI, result.Table = "", "", ""
	result.Embeds = nil
	return result
}

// Identity returns the canonical metadata identity of an independent view.
// Namespace is part of the identity because one component may contain the
// same logical view name in more than one SQL namespace.
func (v *View) Identity() (string, error) {
	if v == nil {
		return "", fmt.Errorf("view is required")
	}
	key := v.Key
	if key.Kind == "" {
		key.Kind = KindView
	}
	if strings.TrimSpace(key.Name) == "" {
		key.Name = v.CanonicalName()
	}
	key.Name = strings.TrimSpace(key.Name)
	if key.Name == "" {
		return "", fmt.Errorf("view name is required")
	}
	return key.String() + "|namespace:" + strings.TrimSpace(v.Namespace), nil
}

type RelationKind string

type MatchStrategy string

const (
	RelationKindSubview RelationKind  = "subview"
	RelationKindDerived RelationKind  = "derived"
	MatchReadAll        MatchStrategy = "read_all"
	MatchReadMatched    MatchStrategy = "read_matched"
	MatchReadDerived    MatchStrategy = "read_derived"
)

// Relation is canonical authored/compiled metadata for an ordinary child
// view. A DerivedView uses the derived relation kind, not a separate view slot
// or lifecycle. Counts and aggregates are examples, not a separate model.
type Relation struct {
	Name            string          `json:"name,omitempty"`
	Kind            RelationKind    `json:"kind,omitempty"`
	Holder          string          `json:"holder,omitempty"`
	Cardinality     Cardinality     `json:"cardinality,omitempty"`
	Join            string          `json:"join,omitempty"`
	ParentNamespace string          `json:"parentNamespace,omitempty"`
	MatchStrategy   MatchStrategy   `json:"matchStrategy,omitempty"`
	View            *View           `json:"view,omitempty"`
	On              []*RelationLink `json:"on,omitempty"`
}

// RelationLink preserves one AST-proven parent/child equality. Go field
// indexes are resolved later by the reader compiler and never enter spec.
type RelationLink struct {
	ParentNamespace string `json:"parentNamespace,omitempty"`
	ParentColumn    string `json:"parentColumn,omitempty"`
	ChildNamespace  string `json:"childNamespace,omitempty"`
	ChildColumn     string `json:"childColumn,omitempty"`
}

type Partitioning struct {
	Type        string   `json:"type,omitempty"`
	Arguments   []string `json:"arguments,omitempty"`
	Concurrency int      `json:"concurrency,omitempty"`
}

type EmbeddedSQLRef struct {
	Path string `json:"path,omitempty"`
	Raw  string `json:"raw,omitempty"`
}

type ViewSource struct {
	Table string `json:"table,omitempty"`
	SQL   string `json:"sql,omitempty"`
	// URI preserves the source resource identity independently of available
	// SQL. Embeds contains only inline substitutions within SQL.
	URI      string            `json:"uri,omitempty"`
	Embeds   []*EmbeddedSQLRef `json:"embeds,omitempty"`
	Controls *ViewControls     `json:"controls,omitempty"`
	Bindings *ViewBindings     `json:"bindings,omitempty"`
}

func (s *ViewSource) Clone() *ViewSource {
	if s == nil {
		return nil
	}
	result := &ViewSource{
		Table: s.Table,
		SQL:   s.SQL,
		URI:   s.URI,
	}
	if len(s.Embeds) > 0 {
		result.Embeds = make([]*EmbeddedSQLRef, 0, len(s.Embeds))
		for _, embed := range s.Embeds {
			if embed == nil {
				result.Embeds = append(result.Embeds, nil)
				continue
			}
			cloned := *embed
			result.Embeds = append(result.Embeds, &cloned)
		}
	}
	result.Controls = s.Controls.Clone()
	result.Bindings = s.Bindings.Clone()
	return result
}
