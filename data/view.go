package data

import (
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/format/text"
)

// View owns a detached spec snapshot plus compiled reader metadata. Compilation
// may enrich the snapshot; after publication the entire graph is immutable.
// Columns and Relations are reader projections (SQL null fallbacks and paired
// field links), while all other structural metadata is owned by spec.View.
// Execution state, collectors, partitioners and native caches live in reader.
type View struct {
	Spec       spec.View       `json:"spec"`
	Connector  string          `json:"connector,omitempty"`
	Columns    []*Column       `json:"columns,omitempty"`
	Cache      *Cache          `json:"cache,omitempty"`
	Relations  []*Relation     `json:"relations,omitempty"`
	CaseFormat text.CaseFormat `json:"-"`
}

// NullsAllowed resolves the absent authored setting to the reader default.
func (v *View) NullsAllowed() bool {
	return v != nil && boolValue(v.Spec.AllowNulls)
}

// IsGroupable resolves the absent authored setting to the reader default.
func (v *View) IsGroupable() bool {
	return v != nil && boolValue(v.Spec.Groupable)
}

type Cache struct {
	Name   string                    `json:"name,omitempty"`
	Warmup *spec.CacheWarmupSettings `json:"warmup,omitempty"`
}

type Relation struct {
	Name        string            `json:"name,omitempty"`
	Kind        spec.RelationKind `json:"kind,omitempty"`
	Holder      string            `json:"holder,omitempty"`
	Cardinality spec.Cardinality  `json:"cardinality,omitempty"`
	On          Links             `json:"-"`
	Of          *RelationRef      `json:"-"`
}

// IsOutput reports whether the relation binds one top-level output holder
// rather than matching rows back to parent items.
func (r *Relation) IsOutput() bool {
	return r != nil &&
		r.Kind == spec.RelationKindDerived &&
		len(r.On) == 0 &&
		r.Of != nil &&
		len(r.Of.On) == 0 &&
		r.Of.View != nil &&
		r.Of.View.Spec.Source != nil
}

func FromComponent(component *spec.Component) *View {
	if component == nil || component.RootView == nil {
		return nil
	}
	return FromView(component, component.RootView)
}

// FromView resolves one canonical view graph with component-level defaults.
// It is used for both the route root and independent DI-backed view reads.
func FromView(component *spec.Component, source *spec.View) *View {
	if source == nil {
		return nil
	}
	ret := fromSpecView(source.Clone(), map[*spec.View]*View{})
	if component == nil {
		return ret
	}
	if source.Source != nil && source.Source.Bindings != nil && source.Source.Bindings.Connector != "" {
		ret.Connector = source.Source.Bindings.Connector
	} else if component.Settings != nil {
		ret.Connector = component.Settings.DefaultConnector
	}
	if component.Settings != nil && component.Settings.Cache != nil {
		settings := component.Settings.Cache
		if ret.Cache == nil && (settings.Name != "" || settings.Warmup != nil) {
			ret.Cache = &Cache{}
		}
		if ret.Cache != nil {
			if ret.Cache.Name == "" {
				ret.Cache.Name = settings.Name
			}
			if ret.Cache.Warmup == nil {
				ret.Cache.Warmup = settings.Warmup.Clone()
			}
		}
	}
	return ret
}

func fromSpecView(source *spec.View, converted map[*spec.View]*View) *View {
	if source == nil {
		return nil
	}
	if existing := converted[source]; existing != nil {
		return existing
	}
	ret := &View{Spec: *source, Relations: []*Relation{}}
	ret.Columns = columnsFromSpec(source.Columns)
	converted[source] = ret
	if source.Source != nil && source.Source.Bindings != nil {
		ret.Connector = source.Source.Bindings.Connector
		if source.Source.Bindings.CacheName != "" {
			ret.Cache = &Cache{Name: source.Source.Bindings.CacheName}
		}
	}
	for _, relation := range source.Relations {
		if relation == nil {
			continue
		}
		child := fromSpecView(relation.View, converted)
		// Keep shared targets and cycles pointed at the same owned snapshot that
		// reader compilation enriches, rather than retaining a second spec node.
		if child != nil {
			relation.View = &child.Spec
		}
		item := &Relation{
			Name: relation.Name, Kind: relation.Kind, Holder: relation.Holder,
			Cardinality: relation.Cardinality, On: Links{},
			Of: &RelationRef{View: child, On: Links{}, MatchStrategy: runtimeMatchStrategy(relation.MatchStrategy)},
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			item.On = append(item.On, NewLink(link.ParentNamespace, link.ParentColumn, ""))
			item.Of.On = append(item.Of.On, NewLink(link.ChildNamespace, link.ChildColumn, ""))
		}
		ret.Relations = append(ret.Relations, item)
	}
	return ret
}

func runtimeMatchStrategy(source spec.MatchStrategy) MatchStrategy {
	if source == spec.MatchReadAll {
		return MatchReadAll
	}
	return MatchSequential
}

func columnsFromSpec(source []*spec.Column) []*Column {
	if source == nil {
		return nil
	}
	result := make([]*Column, 0, len(source))
	for _, item := range source {
		if item == nil {
			continue
		}
		column := &Column{
			Name: item.Name, Column: item.Source, Expression: item.Expression,
			Groupable: boolValue(item.Groupable), DataType: item.DatabaseType, Tag: item.Tag,
		}
		column.ConfigureNullability(item.Nullable, item.Type.Name)
		if item.Type.Pointer {
			column.NullFallback = ""
		}
		column.Codec = item.Codec
		result = append(result, column)
	}
	return result
}

func boolValue(source *bool) bool {
	return source != nil && *source
}
