package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"strings"
)

type Viewlets struct {
	registry map[string]*Viewlet
	keys     []string
}

func (n *Viewlets) Lookup(name string) *Viewlet {
	return n.registry[name]
}
func (n *Viewlets) Each(fn func(viewlet *Viewlet) error) error {
	for _, key := range n.keys {
		ns := n.registry[key]
		if err := fn(ns); err != nil {
			return err
		}
	}
	return nil
}

func (n *Viewlets) Append(viewlet *Viewlet) {
	n.registry[viewlet.Name] = viewlet
	n.keys = append(n.keys, viewlet.Name)
}
func (n *Viewlets) Init(ctx context.Context, query *query.Select, resource *Resource, initFn, setType func(ctx context.Context, n *Viewlet) error) error {
	root := NewViewlet(query.From.Alias, sqlparser.Stringify(query.From.X), nil, resource)
	root.ViewJSONHint = query.From.Comments

	n.Append(root)
	for i := range query.Joins {
		join := query.Joins[i]
		relViewlet := NewViewlet(join.Alias, sqlparser.Stringify(join.With), join, resource)
		relViewlet.ViewJSONHint = join.Comments
		n.Append(relViewlet)
	}
	resource.buildParameterViews()

	if err := n.applyTopLevelDSQLSetting(query, root); err != nil {
		return err
	}
	if err := n.applyViewHintSettings(); err != nil {
		return err
	}
	if err := n.Each(func(viewlet *Viewlet) error {
		if err := initFn(ctx, viewlet); err != nil {
			return fmt.Errorf("failed to init viewlet: %ns, %w", viewlet.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := resource.ensureViewParametersSchema(ctx, setType); err != nil {
		return err
	}
	if err := n.Each(func(viewlet *Viewlet) error {
		if err := setType(ctx, viewlet); err != nil {
			return fmt.Errorf("failed to init viewlet: %v, %w", viewlet.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}

	n.addRelations(query)
	return nil
}

func (n *Viewlets) applyViewHintSettings() error {
	return n.Each(func(namespace *Viewlet) error {
		return namespace.View.applyHintSettings(namespace)
	})
}

func (n Viewlets) addRelations(query *query.Select) {
	for _, join := range query.Joins {
		relation := n.Lookup(join.Alias)
		if relation.IsMetaView() {
			continue
		}
		parentNs := inference.ParentAlias(join)
		parent := n.Lookup(parentNs)

		relation.Spec.Parent = parent.Spec
		cardinality := view.Many
		if inference.IsToOne(join) {
			cardinality = view.One
		}
		relName := parentNs + "_" + join.Alias //TODO check uniqness with heler resolver
		parent.Spec.AddRelation(relName, join, relation.Spec, cardinality)
	}
}

func (n Viewlets) applyTopLevelDSQLSetting(query *query.Select, namespace *Viewlet) error {
	columns := sqlparser.NewColumns(query.List)
	for i := range columns {
		if err := n.updateTopQuerySetting(columns[i], namespace); err != nil {
			return err
		}
	}
	return nil
}

func (n Viewlets) updateTopQuerySetting(column *sqlparser.Column, namespace *Viewlet) error {
	if column.Namespace == "" {
		column.Namespace = namespace.Name
	}
	namespaceForColumn := n.Lookup(column.Namespace)
	if namespaceForColumn == nil {
		return fmt.Errorf("unknown query namespace: %v", column.Namespace)
	}
	if len(column.Except) > 0 {
		namespaceForColumn.Exclude = column.Except
	}
	if column.Comments != "" && strings.Contains(column.Expression, "*") {
		namespaceForColumn.OutputJSONHint = column.Comments
		if err := parser.TryUnmarshalHint(namespaceForColumn.OutputJSONHint, &namespaceForColumn.OutputConfig); err != nil {
			return err
		}
	}
	if column.Tag != "" {
		namespace.Tags[column.Name] = column.Tag
	}
	if column.Type != "" {
		namespace.Casts[column.Name] = column.Type
	}
	return nil
}
