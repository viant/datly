package inference

import (
	"fmt"
	dConfig "github.com/viant/datly/config"
	"github.com/viant/datly/utils/formatter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/sqlparser"
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
	PkFields       []*Field
	columnFields   []*Field
	RelationFields []*Field
	skipped        []*Field
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
	for _, candidate := range t.skipped {
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

func (t *Type) AppendColumnField(column *sqlparser.Column, skipped bool) (*Field, error) {
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
	aType, err := types.LookupType(dConfig.Config.Types.Lookup, column.Type)
	if err != nil {
		return nil, err
	}
	field.Schema = view.NewSchema(aType)
	field.Schema.DataType = aType.Name()
	if skipped {
		t.skipped = append(t.skipped, field)
	} else {
		t.columnFields = append(t.columnFields, field)
	}
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
		relField := specType.RelationFields[i].Field
		relField.Fields = rel.Fields()
		result = append(result, &relField)
	}
	for _, field := range specType.columnFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}
	for _, field := range specType.RelationFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &view.Schema{DataType: "bool"}})
	}
	result = append(result, hasField)
	return result
}

func (t *Type) ColumnFields() []*view.Field {
	var result = make([]*view.Field, 0, 1+len(t.columnFields)+len(t.RelationFields))
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
	t.RelationFields = append(t.RelationFields, field)
	return field
}

func (t *Type) Fields() []reflect.StructField {
	var fields []reflect.StructField
	for _, field := range t.columnFields {
		fields = append(fields, field.StructField())
	}
	return fields
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

					result.RelationFields = append(result.RelationFields, field)
				}
			}
		}
	}
	return result, nil
}

func PkgPath(fieldName string, pkgPath string) (fieldPath string) {
	if fieldName[0] > 'Z' || fieldName[0] < 'A' {
		fieldPath = pkgPath
	}
	return fieldPath
}
