package compile

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
)

// FactoryComposition is compilation-local source-member authority. The factory
// authoring boundary constructs it from build-selected Go declarations. It is
// never serialized into a reader or used by ordinary reader compilation.
type FactoryComposition struct {
	RootSource, RootAlias, RootType string
	Joins                           []FactoryCompositionJoin
	logical                         map[*spec.View]bool
	relations                       map[*spec.Relation]bool
}

type FactoryCompositionJoin struct {
	Source, Alias, ParentAlias, Holder, TypeName string
	Tag                                          string
	Cardinality                                  spec.Cardinality
	Logical                                      bool
}

func (p *FactoryComposition) start(parsed *query.Select, root *spec.View) error {
	if p == nil {
		return nil
	}
	if strings.TrimSpace(sqlparser.Stringify(parsed.From.X)) != p.RootSource || queryNamespace(parsed) != p.RootAlias || p.RootType == "" {
		return fmt.Errorf("factory composition root does not match selected source authority")
	}
	if parsed.Union != nil || len(parsed.WithSelects) != 0 || len(parsed.GroupBy) != 0 || parsed.Having != nil || parsed.Window != nil || parsed.Qualify != nil || len(parsed.OrderBy) != 0 || parsed.Limit != nil || parsed.Offset != nil || strings.TrimSpace(parsed.Kind) != "" {
		return fmt.Errorf("factory composition root cannot own executable SQL clauses")
	}
	p.logical = map[*spec.View]bool{root: true}
	p.relations = map[*spec.Relation]bool{}
	// Construct an empty source before any physical source inference.
	root.Source = &spec.ViewSource{}
	return nil
}

func (p *FactoryComposition) compileJoin(join *query.Join, index int, frame TemplateFrame, parsed *query.Select) (*spec.Relation, error) {
	if p == nil || index >= len(p.Joins) {
		return nil, fmt.Errorf("factory composition join has no source authority")
	}
	proof := p.Joins[index]
	if join == nil || join.Alias != proof.Alias || strings.TrimSpace(sqlparser.Stringify(join.With)) != proof.Source || !strings.EqualFold(strings.Join(strings.Fields(join.Raw), " "), "CROSS JOIN") || (join.On != nil && join.On.X != nil) {
		return nil, fmt.Errorf("factory composition join %d differs from selected source member: %+v expected %+v", index+1, join, proof)
	}
	source := &spec.ViewSource{}
	if !proof.Logical {
		var err error
		source, err = canonicalReadSource(join.With, proof.Alias, parsed.WithSelects, parsed.WithRecursive, frame)
		if err != nil {
			return nil, err
		}
		source.Table, _, err = readSourceTable(join.With, parsed.WithSelects, 0)
		if err != nil {
			return nil, err
		}
		source.TableInferred = true
		if source.Table == "" {
			return nil, fmt.Errorf("factory referenced row requires an independently authored physical source")
		}
	}
	child := &spec.View{Name: proof.Alias, Namespace: proof.Alias, Source: source, Cardinality: proof.Cardinality}
	relation := &spec.Relation{Tag: proof.Tag, Name: proof.Alias, Kind: spec.RelationKindSubview, Holder: proof.Holder, Cardinality: proof.Cardinality, ParentNamespace: proof.ParentAlias, Join: join.Kind, View: child}
	if proof.Logical {
		p.logical[child] = true
	}
	p.relations[relation] = true
	return relation, nil
}

func (p *FactoryComposition) Logical(view *spec.View) bool { return p != nil && p.logical[view] }
func (p *FactoryComposition) admits(relation *spec.Relation) bool {
	return p != nil && p.relations[relation]
}

func (p *FactoryComposition) validate(root *spec.View) error {
	if p == nil {
		return nil
	}
	if root.TypeName != p.RootType {
		return fmt.Errorf("factory composition root type %q differs from source %q", root.TypeName, p.RootType)
	}
	views := canonicalViews(root)
	for _, proof := range p.Joins {
		view := views[strings.ToLower(proof.Alias)]
		if view == nil || view.TypeName != proof.TypeName {
			return fmt.Errorf("factory composition member %s type differs from selected source", proof.Alias)
		}
		parent := views[strings.ToLower(proof.ParentAlias)]
		found := false
		if parent != nil {
			for _, relation := range parent.Relations {
				if relation.View == view && relation.Holder == proof.Holder && relation.Cardinality == proof.Cardinality && relation.Tag == proof.Tag && p.admits(relation) {
					found = true
				}
			}
		}
		if !found {
			return fmt.Errorf("factory composition member %s holder/container differs from source", proof.Alias)
		}
	}
	return nil
}

// Discovery clones only graph headers. Logical sources are absent in this
// detached discovery graph, while physical sources retain their independently
// authored source pointers and fresh schema facts. InputShape is never mutated.
func (p *FactoryComposition) Discovery(component *spec.Component) (*spec.Component, func()) {
	if p == nil {
		return component, func() {}
	}
	result := component.Clone()
	pairs := map[*spec.View]*spec.View{}
	var clone func(*spec.View) *spec.View
	clone = func(view *spec.View) *spec.View {
		if view == nil {
			return nil
		}
		if prior := pairs[view]; prior != nil {
			return prior
		}
		copy := *view
		pairs[view] = &copy
		copy.Relations = nil
		if p.Logical(view) {
			copy.Source = nil
		}
		for _, relation := range view.Relations {
			if relation == nil {
				copy.Relations = append(copy.Relations, nil)
				continue
			}
			edge := *relation
			edge.View = clone(relation.View)
			copy.Relations = append(copy.Relations, &edge)
		}
		return &copy
	}
	result.RootView = clone(component.RootView)
	return result, func() {
		for original, discovered := range pairs {
			if !p.Logical(original) {
				original.Columns = discovered.Columns
				original.Auxiliary = discovered.Auxiliary
			}
		}
	}
}
