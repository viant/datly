package translator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx"
	"strings"
)

var arithmeticOperator = map[string]bool{
	"+": true,
	"-": true,
	"/": true,
	"*": true,
}

type (
	Viewlet struct {
		Name   string
		Holder string

		Connector         string
		SQL               string
		SanitizedSQL      string
		Expanded          *sqlx.SQL
		Resource          *Resource
		Table             *inference.Table
		Join              *query.Join
		Spec              *inference.Spec
		OutputJSONHint    string
		ViewJSONHint      string
		TableJSONHint     string
		Exclude           []string
		Whitelisted       []string
		ColumnConfig      view.ColumnConfigs
		namedColumnConfig view.NamedColumnConfig
		View              *View
		TypeDefinition    *view.TypeDefinition
		OutputSettings
		sourceViewlet *Viewlet
		Columns       view.Columns
		Summary       *Viewlet
		IsSummary     bool
	}

	OutputSettings struct {
		Style       string
		Field       string
		Kind        string
		Cardinality state.Cardinality
		DataType    string
		Title       string
	}
)

func (o *OutputSettings) IsToOne() bool {
	return o.ViewCardinality() == state.One
}

func (o *OutputSettings) ViewCardinality() state.Cardinality {
	if o.Cardinality == "" {
		o.Cardinality = state.Many
	}
	return o.Cardinality
}

func (v *Viewlet) GetConnector() string {
	if v.View != nil {
		if v.View.View.Connector != nil {
			return v.View.View.Connector.Ref
		}
		if v.View.Connector != "" {
			return v.View.Connector
		}
	}
	return v.Connector
}

func (v *Viewlet) IsMetaView() bool {
	return v.sourceViewlet != nil
}

func (v *Viewlet) UpdateParameterType(aState *inference.State, name string, expression *parser.ExpressionContext) {
	parameter := aState.Lookup(name)
	if expression.IsJSONCodec() {
		return
	}
	if index := strings.Index(name, "."); index != -1 && parameter == nil {
		if holder := aState.Lookup(name[:index]); holder != nil {
			return
		}
	}
	if parameter != nil && !parameter.AssumedType && parameter.HasSchema() { //already derived schema from column
		return
	}

	if parameter == nil {
		parameter = &inference.Parameter{}
		parameter.Name = name
		//TODO add default kind and location
		aState.Append(parameter)
	}

	parameter.EnsureLocation()
	if parameter.In.Kind == "" {
		parameter.In.Kind = v.Resource.ImpliedKind()
		parameter.In.Name = name
	}

	switch parameter.In.Kind {
	case state.KindParam, state.KindView:
		return
	}
	parameter.EnsureSchema()
	if expression.Column != "" {
		if column := v.Table.Lookup(expression.Column); column != nil && column.Type != "" {
			parameter.Schema.Name = column.Type
			pkg, ok := v.Resource.typePackages[state.RawComponentType(column.Type)]
			if ok {
				parameter.Schema.SetPackage(pkg)
			}
		}
	}
	if elements := expression.BeforeElements(); len(elements) > 0 {
		operator, column := v.extractUsageInfo(elements, name)
		if column != nil && column.Type != "" {
			parameter.Schema.DataType = column.Type
		}
		if operator == "in" {
			parameter.Schema.Cardinality = state.Many
		}
	}
	if parameter.Schema.DataType != "" {
		return
	}
	if expression.Type != nil {
		parameter.Schema = state.NewSchema(expression.Type)
		parameter.Schema.DataType = expression.Type.String()
		parameter.AssumedType = true
	}

}

func (v *Viewlet) excludeMap() map[string]bool {
	if len(v.Exclude) == 0 {
		return map[string]bool{}
	}
	var result = make(map[string]bool)
	for _, item := range v.Exclude {
		result[item] = true
	}
	return result
}

func (v *Viewlet) whitelistMap() map[string]bool {
	if len(v.Whitelisted) == 0 {
		return map[string]bool{}
	}
	var result = make(map[string]bool)
	for _, item := range v.Whitelisted {
		result[item] = true
	}
	return result
}

func (v *Viewlet) extractUsageInfo(elements []string, name string) (string, *sqlparser.Column) {
	operator := ""
	var column *sqlparser.Column
	operatorIndex := -1
outer:
	for i := len(elements) - 1; i >= 0; i-- {
		candidate := strings.ToLower(elements[i])
		switch candidate {
		case "=", ">", "<", "/", "*", "+", "-", ">=", "<=", "!=", "in":
			operator = candidate
			operatorIndex = i
			break outer
		case "cast":
			return operator, v.Table.Lookup(name)
		}
	}

	if arithmeticOperator[operator] {
		if column = v.Table.Lookup(elements[operatorIndex-1]); column != nil {
			return operator, column
		}
	}
	for i := operatorIndex - 1; i >= 0; i-- {
		if column = v.Table.Lookup(elements[i]); column != nil {
			return operator, column
		}
	}
	return operator, nil
}

func NewViewlet(name, SQL string, join *query.Join, resource *Resource) *Viewlet {
	SQL = inference.TrimParenthesis(SQL)
	connector := ExtractConnectorRef(&SQL)
	ret := &Viewlet{
		Name:      name,
		SQL:       SQL,
		Join:      join,
		Exclude:   nil,
		Resource:  resource,
		View:      &View{Namespace: name, View: view.View{Name: name}},
		Connector: connector,
	}
	return ret
}

func (v *Viewlet) discoverTables(ctx context.Context, db *sql.DB, SQL string) (err error) {
	v.Table, err = inference.NewTable(ctx, db, SQL)
	if v.Table != nil {
		for _, column := range v.Table.QueryColumns {
			name := column.Alias
			if name == "" {
				name = column.Name
			}
			v.Whitelisted = append(v.Whitelisted, strings.ToLower(name))
			columnConfig, err := inference.ExtractColumnConfig(column)
			if err != nil {
				return err
			}
			if columnConfig != nil {
				v.ColumnConfig = append(v.ColumnConfig, columnConfig)
			}
		}
	}
	//Whitelisted
	for _, config := range v.ColumnConfig {
		if config.DataType == nil {
			continue
		}
		if column := v.Table.Lookup(config.Name); column != nil {
			column.Type = *config.DataType
		}
	}
	return err
}

func (v *Viewlet) applyOutputShorthands() {
	if v.DataType != "" {
		if v.View.Schema == nil {
			v.View.Schema = &state.Schema{}
		}
		setter.SetStringIfEmpty(&v.View.Schema.DataType, v.DataType)
	}
}

func (v *Viewlet) mergeTableJSONHint(hint string) error {
	v.TableJSONHint = hint
	var output OutputSettings
	if err := inference.TryUnmarshalHint(v.TableJSONHint, &output); err != nil {
		return fmt.Errorf("invalid table %v hint: %s,  %w", v.Table.Name, v.TableJSONHint, err)
	}
	data, _ := parser.MergeStructs(&output, &v.OutputSettings)
	return json.Unmarshal(data, &v.OutputSettings)
}
