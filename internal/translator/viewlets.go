package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
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
func (n *Viewlets) Init(ctx context.Context, aQuery *query.Select, resource *Resource, initFn, setType func(ctx context.Context, n *Viewlet) error) error {
	root := NewViewlet(aQuery.From.Alias, sqlparser.Stringify(aQuery.From.X), nil, resource)
	root.ViewJSONHint = aQuery.From.Comments
	if root.ViewJSONHint == "" && aQuery.From.X != nil {
		if rawExpr, ok := aQuery.From.X.(*qexpr.Raw); ok {
			if querySelect, ok := rawExpr.X.(*query.Select); ok {
				root.ViewJSONHint = querySelect.From.Comments
			}
		}
	}
	n.Append(root)
	for i := range aQuery.Joins {
		join := aQuery.Joins[i]
		relViewlet := NewViewlet(join.Alias, sqlparser.Stringify(join.With), join, resource)
		relViewlet.ViewJSONHint = join.Comments
		n.Append(relViewlet)
	}
	resource.buildParameterViews()

	if err := n.applyTopLevelDSQLSetting(aQuery, root); err != nil {
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
	if err := resource.ensurePathParametersSchema(ctx); err != nil {
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

	n.addRelations(aQuery)
	return nil
}

func (n *Viewlets) applyViewHintSettings() error {
	return n.Each(func(namespace *Viewlet) error {
		return namespace.View.applyHintSettings(namespace)
	})
}

func (n *Viewlets) addRelations(query *query.Select) {
	for _, join := range query.Joins {
		relation := n.Lookup(join.Alias)
		if relation.IsMetaView() {
			continue
		}
		parentNs := inference.ParentAlias(join)
		parentViewlet := n.Lookup(parentNs)

		relation.Spec.Parent = parentViewlet.Spec
		cardinality := state.Many
		if inference.IsToOne(join) || relation.OutputSettings.IsToOne() {
			cardinality = state.One
		}
		relName := join.Alias
		parentViewlet.Spec.AddRelation(relName, join, relation.Spec, cardinality)
	}
}

func (n *Viewlets) applyTopLevelDSQLSetting(query *query.Select, namespace *Viewlet) error {
	columns := sqlparser.NewColumns(query.List)
	for i := range columns {
		if err := n.applyTopLevelSetting(columns[i], namespace); err != nil {
			return err
		}
	}
	return nil
}

func (n *Viewlets) applyTopLevelSetting(column *sqlparser.Column, viewlet *Viewlet) error {
	done, err := n.applySettingFunctions(column)
	if done || err != nil {
		return err
	}
	if column.Namespace == "" {
		column.Namespace = viewlet.Name
	}
	namespaceForColumn := n.Lookup(column.Namespace)
	if namespaceForColumn == nil {
		return fmt.Errorf("unknown query viewlet: %v", column.Namespace)
	}
	if len(column.Except) > 0 {
		namespaceForColumn.Exclude = column.Except
	}
	if column.Comments != "" && strings.Contains(column.Expression, "*") {
		namespaceForColumn.OutputJSONHint = column.Comments
		if err := inference.TryUnmarshalHint(namespaceForColumn.OutputJSONHint, &namespaceForColumn.OutputSettings); err != nil {
			return err
		}
		if namespaceForColumn.OutputSettings.Field != "" {
			viewlet.Resource.Rule.Route.Output.Field = namespaceForColumn.OutputSettings.Field
		}
		if namespaceForColumn.OutputSettings.Cardinality != "" {
			if viewlet.View.Schema == nil {
				viewlet.View.Schema = &state.Schema{}
			}
			viewlet.View.Schema.Cardinality = namespaceForColumn.OutputSettings.Cardinality
		}
	}

	if column.Tag != "" {
		viewlet.Tags[column.Name] = column.Tag
	}
	if column.Type != "" {
		viewlet.Casts[column.Name] = column.Type
	}
	return nil
}

func extractFunction(column *sqlparser.Column) (string, []string) {
	fnName := ""
	var args []string
	if index := strings.Index(column.Expression, "("); index != -1 {
		fnName = column.Expression[:index]
		arg := column.Expression[index+1 : len(column.Expression)-2]
		for _, item := range strings.Split(arg, ",") {
			args = append(args, strings.TrimSpace(item))
		}
	}
	return fnName, args
}
