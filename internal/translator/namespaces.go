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

type Namespaces struct {
	registry map[string]*Namespace
	keys     []string
}

func (n *Namespaces) Lookup(name string) *Namespace {
	return n.registry[name]
}
func (n *Namespaces) Each(fn func(namespace *Namespace) error) error {
	for _, key := range n.keys {
		ns := n.registry[key]
		if err := fn(ns); err != nil {
			return err
		}
	}
	return nil
}

func (n *Namespaces) Append(namespace *Namespace) {
	n.registry[namespace.Name] = namespace
	n.keys = append(n.keys, namespace.Name)
}
func (n *Namespaces) Init(ctx context.Context, query *query.Select, resource *Resource, initFn, setType func(ctx context.Context, n *Namespace) error) error {
	rootNamespace := NewNamespace(query.From.Alias, sqlparser.Stringify(query.From.X), nil, resource)
	rootNamespace.ViewJSONHint = query.From.Comments

	n.Append(rootNamespace)
	for i := range query.Joins {
		join := query.Joins[i]
		relNamespace := NewNamespace(join.Alias, sqlparser.Stringify(join.With), join, resource)
		relNamespace.ViewJSONHint = join.Comments
		n.Append(relNamespace)
	}
	for _, parameter := range resource.State.FilterByKind(view.KindDataView) {
		namespace := NewNamespace(parameter.Name, parameter.SQL, nil, resource)
		namespace.Cardinality = view.One
		n.Append(namespace)
	}

	if err := n.applyTopLevelDSQLSetting(query, rootNamespace); err != nil {
		return err
	}
	if err := n.applyViewHintSettings(); err != nil {
		return err
	}
	if err := n.Each(func(namespace *Namespace) error {
		if err := initFn(ctx, namespace); err != nil {
			return fmt.Errorf("failed to init namespace: %ns, %w", namespace.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := resource.ensureViewParametersSchema(ctx, setType); err != nil {
		return err
	}
	if err := n.Each(func(namespace *Namespace) error {
		if err := setType(ctx, namespace); err != nil {
			return fmt.Errorf("failed to init namespace: %v, %w", namespace.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}
	n.addRelations(query)
	return nil
}

func (n *Namespaces) applyViewHintSettings() error {
	return n.Each(func(namespace *Namespace) error {
		return namespace.View.applyHintSettings(namespace)
	})
}

func (n Namespaces) addRelations(query *query.Select) {
	for _, join := range query.Joins {
		parentNs := inference.ParentAlias(join)
		parent := n.Lookup(parentNs)
		relation := n.Lookup(join.Alias)
		relation.Spec.Parent = parent.Spec
		cardinality := view.Many
		if inference.IsToOne(join) {
			cardinality = view.One
		}
		relName := parentNs + "_" + join.Alias //TODO check uniqness with heler resolver
		parent.Spec.AddRelation(relName, join, relation.Spec, cardinality)
	}
}

func (n Namespaces) applyTopLevelDSQLSetting(query *query.Select, namespace *Namespace) error {
	columns := sqlparser.NewColumns(query.List)
	for i := range columns {
		if err := n.updateTopQuerySetting(columns[i], namespace); err != nil {
			return err
		}
	}
	return nil
}

func (n Namespaces) updateTopQuerySetting(column *sqlparser.Column, namespace *Namespace) error {
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
	if column.Comments != "" && strings.Contains(column.Name, "*") {
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
