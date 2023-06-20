package codegen

import (
	"fmt"
	dConfig "github.com/viant/datly/config"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
	qexpr "github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/toolbox/format"
	"strings"
)

type Field struct {
	view.Field
	Column     *sink.Column
	Tags       Tags
	Ptr        bool
	ColumnCase format.Case
}

type Type struct {
	Name          string
	Cardinality   view.Cardinality
	pkFields      []*Field
	columnFields  []*Field
	relationField []*Field
}

func (t *Type) AppendColumnField(column *sink.Column) (*Field, error) {
	columnCase, err := format.NewCase(formatter.DetectCase(column.Name))
	if err != nil {
		return nil, err
	}
	field := &Field{Column: column,
		ColumnCase: columnCase,
		Field:      view.Field{Name: columnCase.Format(column.Name, format.CaseUpperCamel)},
		Ptr:        column.IsNullable(),
		Tags:       Tags{},
	}
	aType, err := types.GetOrParseType(dConfig.Config.LookupType, column.Type)
	if err != nil {
		return nil, err
	}
	field.Schema = view.NewSchema(aType)
	field.Schema.DataType = aType.Name()
	t.columnFields = append(t.columnFields, field)
	return field, nil
}

func (t *Type) Fields() []*view.Field {
	result := t.ColumnFields()
	for i := range t.relationField {
		result = append(result, &t.relationField[i].Field)
	}
	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(`setMarker:"true" typeName:"%t" json:"-"  sqlx:"-" `, t.Name+"Has"),
		Ptr:  true,
	}
	for _, field := range t.columnFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}
	result = append(result, hasField)
	return result
}

func (t *Type) ColumnFields() []*view.Field {
	var result = make([]*view.Field, 0, 1+len(t.columnFields)+len(t.relationField))
	for i := range t.columnFields {
		field := t.columnFields[i].Field
		result = append(result, &field)
	}
	return result
}

func (t *Type) AddRelation(name string, graph *Spec, join *query.Join) *Field {
	card := cardinality(join)
	var field = &Field{Field: view.Field{
		Name:        name,
		Cardinality: card,
		Ptr:         card == view.One,
	}}
	field.Tags.Set("typeName", TagValue{graph.Type.Name})
	field.Tags.Set("sqlx", TagValue{"-"})
	field.Tags.buildRelation(graph, join)
	field.Tag = field.Tags.Stringify()
	t.relationField = append(t.relationField, field)
	return field
}

func cardinality(join *query.Join) view.Cardinality {
	if join == nil {
		return view.Many
	}
	if strings.Contains(sqlparser.Stringify(join.On), "1 = 1") {
		return view.One
	}
	return view.Many
}

func extractRelationColumns(join *query.Join) (string, string) {
	relColumn := ""
	refColumn := ""
	sqlparser.Traverse(join.On, func(n node.Node) bool {
		switch actual := n.(type) {
		case *qexpr.Binary:
			if xSel, ok := actual.X.(*qexpr.Selector); ok {
				if xSel.Name == join.Alias {
					refColumn = sqlparser.Stringify(xSel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(xSel.X)
				}
			}
			if ySel, ok := actual.Y.(*qexpr.Selector); ok {
				if ySel.Name == join.Alias {
					refColumn = sqlparser.Stringify(ySel.X)
				} else if relColumn == "" {
					relColumn = sqlparser.Stringify(ySel.X)
				}
			}
			return true
		}
		return true
	})
	return relColumn, refColumn
}
