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
	Namespace struct {
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
		OutputConfig
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

func (n *Namespace) UpdateParameterType(state *inference.State, name string, expression *parser.ExpressionContext) {
	parameter := state.Lookup(name)
	if parameter == nil {
		parameter = &inference.Parameter{}
		parameter.Name = name
		//TODO add default kind and location
		state.Append(parameter)
	}
	parameter.EnsureSchema()
	if expression.Column != "" {
		if column := n.Table.Lookup(expression.Column); column != nil && column.Type != "" {
			parameter.Schema.DataType = column.Type
		}
	}
	if elements := expression.BeforeElements(); len(elements) > 0 {
		operator, column := n.extractUsageInfo(elements, name)
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
	}
}

func (n *Namespace) excludeMap() map[string]bool {
	if len(n.Exclude) == 0 {
		return map[string]bool{}
	}
	var result = make(map[string]bool)
	for _, item := range n.Exclude {
		result[item] = true
	}
	return result
}

func (n *Namespace) whitelistMap() map[string]bool {
	if len(n.Whitelisted) == 0 {
		return map[string]bool{}
	}
	var result = make(map[string]bool)
	for _, item := range n.Whitelisted {
		result[item] = true
	}
	return result
}

func (n *Namespace) extractUsageInfo(elements []string, name string) (string, *sqlparser.Column) {
	operator := ""
	var column *sqlparser.Column
	operatorIndex := -1
	for i := len(elements) - 1; i >= 0; i-- {
		candidate := strings.ToLower(elements[i])
		switch candidate {
		case "=", ">", "<", "/", "*", "+", "-", ">=", "<=", "!=", "in":
			operator = candidate
			operatorIndex = i
			break
		case "cast":
			return operator, n.Table.Lookup(name)
		}
	}

	if arithmeticOperator[operator] {
		if column = n.Table.Lookup(elements[operatorIndex-1]); column != nil {
			return operator, column
		}
	}
	for i := operatorIndex - 1; i >= 0; i-- {
		if column = n.Table.Lookup(elements[i]); column != nil {
			return operator, column
		}
	}
	return operator, nil
}

func NewNamespace(name, SQL string, join *query.Join, resource *Resource) *Namespace {
	SQL = inference.TrimParenthesis(SQL)
	connector := ExtractConnectorRef(&SQL)
	ret := &Namespace{
		Name:       name,
		SQL:        SQL,
		Join:       join,
		Exclude:    nil,
		Resource:   resource,
		Transforms: map[string]*Function{},
		Tags:       map[string]string{},
		Casts:      map[string]string{},
		View:       &View{Namespace: name},
		Connector:  connector,
	}
	return ret
}

func (n *Namespace) discoverTables(ctx context.Context, db *sql.DB, SQL string) (err error) {
	n.Table, err = inference.NewTable(ctx, db, SQL)
	if n.Table != nil {
		for _, column := range n.Table.QueryColumns {
			n.Whitelisted = append(n.Whitelisted, column.Alias)
			if column.Comments != "" {
				columnConfig := &view.ColumnConfig{}
				if err := parser.TryUnmarshalHint(column.Comments, columnConfig); err != nil {
					return fmt.Errorf("invalid column %v settings: %w, %s", column.Name, err, column.Comments)
				}
				columnConfig.Name = column.Name
				n.ColumnConfig = append(n.ColumnConfig, columnConfig)
			}
		}
	}

	//Whitelisted
	for name, dataType := range n.Casts {
		if column := n.Table.Lookup(name); column != nil {
			column.Type = dataType
		}
	}
	return err
}
