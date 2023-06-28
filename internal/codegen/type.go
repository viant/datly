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
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	"reflect"
)

func (f *Field) StructField() reflect.StructField {
	return reflect.StructField{
		Name: f.Name,
		Tag:  reflect.StructTag(f.Tag),
		Type: f.Field.Schema.Type(),
	}
}

type Type struct {
	Package        string
	Name           string
	Cardinality    view.Cardinality
	pkFields       []*Field
	columnFields   []*Field
	relationFields []*Field
}

func (t *Type) ByColumn(name string) *Field {
	for _, candidate := range t.columnFields {
		if column := candidate.Column; column != nil && column.Alias != "" && column.Alias == name {
			return candidate
		}
	}
	for _, candidate := range t.columnFields {
		if column := candidate.Column; column != nil && column.Name != "" && column.Name == name {
			return candidate
		}
	}
	return nil
}

func (t *Type) TypeName() string {
	return t.ExpandType(t.Name)
}

func (t *Type) ExpandType(simpleName string) string {
	pkg := t.Package
	if pkg == "" {
		pkg = "autogen"
	}
	return pkg + "." + simpleName
}

func (t *Type) AppendColumnField(column *sqlparser.Column) (*Field, error) {
	columnCase, err := format.NewCase(formatter.DetectCase(column.Name))
	if err != nil {
		return nil, err
	}

	fieldName := column.Alias
	if fieldName == "" {
		fieldName = column.Name
	}
	field := &Field{Column: column,
		ColumnCase: columnCase,
		Field:      view.Field{Name: columnCase.Format(fieldName, format.CaseUpperCamel)},
		Ptr:        column.IsNullable,
		Tags:       Tags{},
	}
	if column.Type == "" {
		return nil, fmt.Errorf("failed to match type: %v %v %v\n", column.Alias, column.Name, column.Expression)
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
		if t.columnFields[i].Column.IsNullable {
			field.Ptr = true
		}
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

func (t *Type) Fields() []reflect.StructField {
	var fields []reflect.StructField
	for _, field := range t.columnFields {
		fields = append(fields, field.StructField())
	}

	return fields
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
				if structology.IsSetMarker(rField.Tag) {
					continue
				}

				if typeName, _ := rField.Tag.Lookup("typeName"); typeName != "" {

					result.relationFields = append(result.relationFields, field)
				}
			}
		}
	}
	return result, nil
}
