package translator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view"
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
		Name           string
		Holder         string
		Connector      string
		SQL            string
		SanitizedSQL   string
		Expanded       *sqlx.SQL
		Resource       *Resource
		Table          *inference.Table
		Join           *query.Join
		Spec           *inference.Spec
		OutputJSONHint string
		ViewJSONHint   string
		Exclude        []string
		Whitelisted    []string
		Casts          map[string]string
		Tags           map[string]string
		Transforms     map[string]*Function
		ColumnConfig   []*view.ColumnConfig
		View           *View

		TypeDefinition *view.TypeDefinition
		OutputConfig
		sourceViewlet *Viewlet
	}

	Function struct {
		Name string
		Args []string
	}

	OutputConfig struct {
		Style       string
		Field       string
		Kind        string
		Cardinality view.Cardinality
	}
)

func (o *OutputConfig) IsToOne() bool {
	return o.ViewCardinality() == view.One
}

func (o *OutputConfig) ViewCardinality() view.Cardinality {
	if o.Cardinality == "" {
		o.Cardinality = view.Many
	}
	return o.Cardinality
}

func (v *Viewlet) IsMetaView() bool {
	return v.sourceViewlet != nil
}

func (v *Viewlet) UpdateParameterType(state *inference.State, name string, expression *parser.ExpressionContext) {
	parameter := state.Lookup(name)
	if index := strings.Index(name, "."); index != -1 && parameter == nil {
		if holder := state.Lookup(name[:index]); holder != nil {
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
		state.Append(parameter)
	}

	parameter.EnsureLocation()
	if parameter.In.Kind == "" {
		parameter.In.Kind = v.Resource.ImpliedKind()
		parameter.In.Name = name
	}

	switch parameter.In.Kind {
	case view.KindParam, view.KindDataView:
		return
	}
	parameter.EnsureSchema()
	if expression.Column != "" {
		if column := v.Table.Lookup(expression.Column); column != nil && column.Type != "" {
			parameter.Schema.DataType = column.Type
		}
	}
	if elements := expression.BeforeElements(); len(elements) > 0 {
		operator, column := v.extractUsageInfo(elements, name)
		if column != nil && column.Type != "" {
			parameter.Schema.DataType = column.Type
		}
		if operator == "in" {
			parameter.Schema.Cardinality = view.Many
		}
	}
	if parameter.Schema.DataType != "" {
		return
	}
	if expression.Type != nil {
		parameter.Schema = view.NewSchema(expression.Type)
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
		Name:       name,
		SQL:        SQL,
		Join:       join,
		Exclude:    nil,
		Resource:   resource,
		Transforms: map[string]*Function{},
		Tags:       map[string]string{},
		Casts:      map[string]string{},
		View:       &View{Namespace: name, View: view.View{Name: name}},
		Connector:  connector,
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
			if column.Comments != "" {
				columnConfig := &view.ColumnConfig{}
				if err := parser.TryUnmarshalHint(column.Comments, columnConfig); err != nil {
					return fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
				}
				columnConfig.Name = column.Name
				v.ColumnConfig = append(v.ColumnConfig, columnConfig)
			}
		}
	}

	//Whitelisted
	for name, dataType := range v.Casts {
		if column := v.Table.Lookup(name); column != nil {
			column.Type = dataType
		}
	}
	return err
}
