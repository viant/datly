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
	"reflect"
	"strings"
)

type Type struct {
	Package        string
	Name           string
	Cardinality    view.Cardinality
	pkFields       []*Field
	columnFields   []*Field
	relationFields []*Field
}

func (t *Type) ByColumn(columnName string) *Field {
	for _, candidate := range t.columnFields {
		if column := candidate.Column; column != nil && column.Name == columnName {
			return candidate
		}
	}
	return nil
}

func (t *Type) TypeName() string {
	return t.expandType(t.Name)
}

func (t *Type) expandType(simpleName string) string {
	pkg := t.Package
	if pkg == "" {
		pkg = "autogen"
	}
	return pkg + "." + simpleName
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

func (s *Spec) Fields() []*view.Field {
	specType := s.Type
	result := specType.ColumnFields()
	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(`setMarker:"true" typeName:"%v" json:"-"  sqlx:"-" `, specType.Name+"Has"),
		Ptr:  true,
	}

	for i, rel := range s.Relations {
		relField := specType.relationFields[i].Field
		relField.Fields = rel.Fields()
		result = append(result, &relField)
	}
	for _, field := range specType.columnFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}
	for _, field := range specType.relationFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}
	result = append(result, hasField)
	return result
}

func (t *Type) ColumnFields() []*view.Field {
	var result = make([]*view.Field, 0, 1+len(t.columnFields)+len(t.relationFields))
	for i := range t.columnFields {
		field := t.columnFields[i].Field
		result = append(result, &field)
	}
	return result
}

func (t *Type) AddRelation(name string, spec *Spec, relation *Relation) *Field {
	var field = &Field{Field: view.Field{
		Name:        name,
		Cardinality: relation.Cardinality,
		Ptr:         true,
	}}
	field.Tags.Set("typeName", TagValue{spec.Type.Name})
	field.Tags.Set("sqlx", TagValue{"-"})
	field.Tags.buildRelation(spec, relation)
	field.Tag = field.Tags.Stringify()
	t.relationFields = append(t.relationFields, field)
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

func NewType(packageName string, name string, rType reflect.Type) (*Type, error) {
	var result = &Type{Package: packageName, Name: name}
	rType = types.EnsureStruct(rType)
	if rType.NumField() == 1 {
		wrapperField := rType.Field(0)
		typeName, _ := wrapperField.Tag.Lookup("typeName")
		structType := types.EnsureStruct(wrapperField.Type)
		return NewType(packageName, typeName, structType)
	}

	for i := 0; i < rType.NumField(); i++ {
		rField := rType.Field(i)
		field := NewField(&rField)
		if field.Column != nil {
			result.columnFields = append(result.columnFields, field)
		} else {
			rType := field.Schema.Type()
			if rType.Kind() == reflect.Ptr {
				rType = rType.Elem()
			}
			switch rType.Kind() {
			case reflect.Slice, reflect.Struct:
				if typeName, _ := rField.Tag.Lookup("typeName"); typeName != "" {

					result.relationFields = append(result.relationFields, field)
				}
			}
		}
	}
	return result, nil
}
