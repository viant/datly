package translator

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"github.com/viant/tagly/tags"
	"github.com/viant/xreflect"
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
func (n *Viewlets) Init(ctx context.Context, aQuery *query.Select, resource *Resource, initFn, setType func(ctx context.Context, n *Viewlet, aDoc state.Documentation) error) error {

	SQL, err := SafeQueryStringify(aQuery)
	if err != nil {
		return err
	}
	root := NewViewlet(aQuery.From.Alias, SQL, nil, resource)
	root.ViewJSONHint = aQuery.From.Comments
	if root.ViewJSONHint == "" && aQuery.From.X != nil {
		if rawExpr, ok := aQuery.From.X.(*expr.Raw); ok {
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
		if err := initFn(ctx, viewlet, resource.Rule.Doc.Columns); err != nil {
			return fmt.Errorf("failed to init viewlet: %ns, %w", viewlet.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := resource.ensureViewParametersSchema(ctx, setType, resource.Rule.Doc.Columns); err != nil {
		return err
	}
	if err := resource.ensurePathParametersSchema(ctx, resource.State); err != nil {
		return err
	}

	if err := n.Each(func(viewlet *Viewlet) error {
		if err := setType(ctx, viewlet, resource.Rule.Doc.Columns); err != nil {
			return fmt.Errorf("failed to init viewlet: %v, %w", viewlet.Name, err)
		}
		return nil
	}); err != nil {
		return err
	}

	n.addRelations(aQuery)
	return nil
}

func SafeQueryStringify(aQuery *query.Select) (SQL string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid dql: %v", r)
		}
	}()
	SQL = sqlparser.Stringify(aQuery.From.X)
	return SQL, err
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
	done, err := n.applySettingFunctions(column, viewlet.Name)
	if done || err != nil {
		return err
	}
	if column.Namespace == "" {
		column.Namespace = viewlet.Name
	}
	columnViewlet := n.Lookup(column.Namespace)
	if columnViewlet == nil {
		return fmt.Errorf("unknown query viewlet: %v", column.Namespace)
	}
	if len(column.Except) > 0 {
		columnViewlet.Exclude = column.Except
	}
	if column.Comments != "" && strings.Contains(column.Expression, "*") {
		columnViewlet.OutputJSONHint = column.Comments
		if err := inference.TryUnmarshalHint(columnViewlet.OutputJSONHint, &columnViewlet.OutputSettings); err != nil {
			return err
		}
		if columnViewlet.OutputSettings.Field != "" {
			viewlet.Resource.Rule.Route.Output.Field = columnViewlet.OutputSettings.Field
		}
		if columnViewlet.OutputSettings.Cardinality != "" {
			if viewlet.View.Schema == nil {
				viewlet.View.Schema = &state.Schema{}
			}
			viewlet.View.Schema.Cardinality = columnViewlet.OutputSettings.Cardinality
		}
	}

	columnName := column.Name
	//TODO move it to the cast logic  - MASTER LOCATION OR LOGIC PLEASE
	if columnName == viewlet.Name && strings.Contains(column.Expression, "cast(") {
		if column.Type != "string" {
			viewlet.updateViewSchema(column.Type)
		}
		return nil
	}

	_, columnName = namespacedColumn(columnName)
	if columnName == "" {
		return nil
	}
	columnConfig := columnViewlet.columnConfig(columnName)
	if tag := column.Tag; tag != "" {
		tag = trimQuotes(strings.TrimSpace(tag))
		columnConfig.Tag = &tag
	}
	return nil
}

func namespacedColumn(columnName string) (string, string) {
	var ns string
	if index := strings.Index(columnName, "."); index != -1 {
		ns = columnName[:index]
		columnName = columnName[index+1:]
	}
	return ns, columnName
}

func (v *Viewlet) columnConfig(columnName string) *view.ColumnConfig {
	namedConfig := v.namedColumnConfig
	if len(namedConfig) == 0 {
		v.namedColumnConfig = v.ColumnConfig.Index()
		namedConfig = v.namedColumnConfig
	}
	columnConfig, ok := namedConfig[columnName]
	if !ok {
		columnConfig = &view.ColumnConfig{Name: columnName}
		namedConfig[columnName] = columnConfig
		v.ColumnConfig = append(v.ColumnConfig, columnConfig)
	}
	return columnConfig
}

func trimQuotes(tag string) string {
	if (tag[0] == '\'' && tag[len(tag)-1] == '\'') || (tag[0] == '"' && tag[len(tag)-1] == '"') {
		tag = tag[1 : len(tag)-1]
	}
	return tag
}

func (v *Viewlet) updateViewSchema(typeName string) {
	if v.View.Schema == nil {
		v.View.Schema = &state.Schema{}
	}
	pkg := v.Resource.typePackages[state.RawComponentType(typeName)]
	v.View.Schema.Name = typeName
	v.View.Schema.SetPackage(pkg)
	if rType, err := v.Resource.typeRegistry.Lookup(typeName, xreflect.WithPackage(pkg)); err == nil {
		v.View.Schema.SetType(rType)
	}
}

func extractFunction(column *sqlparser.Column) (string, []string) {
	fnName := ""
	var args []string
	if index := strings.Index(column.Expression, "("); index != -1 {
		fnName = column.Expression[:index]
		exprArgs := column.Expression[index+1 : len(column.Expression)-1]

		lcArgs := strings.ToLower(exprArgs)
		if index := strings.Index(lcArgs, " as "); index != -1 {
			name := trimQuotes(strings.TrimSpace((exprArgs[:index])))
			typeName := trimQuotes(strings.TrimSpace((exprArgs[index+4:])))
			args = append(args, name, typeName)
			return fnName, args
		}
		values := tags.Values(exprArgs)
		_ = values.Match(func(item string) error {
			arg := trimQuotes(strings.TrimSpace(item))
			args = append(args, arg)
			return nil
		})
	}
	return fnName, args
}
