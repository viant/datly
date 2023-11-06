package inference

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	dConfig "github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"github.com/viant/sqlparser"
	"github.com/viant/structology"
	"github.com/viant/tagly/format/text"
	"reflect"
)

type reflectOptions struct {
	withTag bool
}

type ReflectOption func(r *reflectOptions)

func WithStructTag() ReflectOption {
	return func(r *reflectOptions) {
		r.withTag = true
	}
}

func (f *Field) StructField(opts ...ReflectOption) reflect.StructField {
	ret := reflect.StructField{
		Name: f.Name,
		Type: f.Field.Schema.Type(),
	}
	options := &reflectOptions{}
	for _, apply := range opts {
		apply(options)
	}
	if options.withTag {
		ret.Tag = reflect.StructTag(f.Tag)
	}
	return ret
}

type Type struct {
	Package        string
	Name           string
	Cardinality    state.Cardinality
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

func (t *Type) AppendColumnField(column *sqlparser.Column, skipped bool, doc state.Documentation, table string) (*Field, error) {
	columnNameOrAlias := column.Alias
	if columnNameOrAlias == "" {
		columnNameOrAlias = column.Name
	}
	columnCase := text.DetectCaseFormat(columnNameOrAlias)
	if !columnCase.IsDefined() {
		return nil, fmt.Errorf("unable to detect case format for: '%s'", column.Name)
	}
	field := &Field{Column: column,
		ColumnCase: columnCase,
		Field:      view.Field{Name: columnCase.Format(columnNameOrAlias, text.CaseFormatUpperCamel)},
		Ptr:        column.IsNullable,
		Tags:       Tags{},
	}

	if doc != nil {
		if fieldDoc, ok := doc.ColumnDocumentation(table, field.Column.Name); ok {
			field.Tags.Set(tags.DocumentationTag, TagValue{fieldDoc})
		}
	}
	if column.Type == "" {
		return nil, fmt.Errorf("failed to match type: %v %v %v\n", column.Alias, column.Name, column.Expression)
	}
	aType, err := types.LookupType(dConfig.Config.Types.Lookup, column.Type)
	if err != nil {
		return nil, err
	}
	field.Schema = state.NewSchema(aType)
	field.Schema.DataType = aType.Name()
	field.Schema.SetPackage(t.Package)
	if skipped {
		field.Skipped = skipped
		t.skipped = append(t.skipped, field)
	} else {
		t.columnFields = append(t.columnFields, field)
	}
	return field, nil
}

func (s *Spec) Fields(includeHas bool, doc state.Documentation) []*view.Field {
	specType := s.Type
	result := specType.ColumnFields(s.Table, doc)

	hasFieldName := "Has"
	hasField := &view.Field{
		Name: hasFieldName,
		Tag:  fmt.Sprintf(state.TypedSetMarkerTag, specType.Name+"Has"),
		Ptr:  true,
	}

	for i, rel := range s.Relations {
		relField := specType.RelationFields[i].Field
		relField.Fields = rel.Fields(includeHas, doc)
		result = append(result, &relField)
	}

	for _, field := range specType.columnFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &state.Schema{DataType: "bool"}})
	}
	for _, field := range specType.RelationFields {
		hasField.Fields = append(hasField.Fields, &view.Field{Name: field.Name, Schema: &state.Schema{DataType: "bool"}})
	}

	if includeHas {
		result = append(result, hasField)
	}
	return result
}

func (t *Type) ColumnFields(table string, doc state.Documentation) []*view.Field {
	var result = make([]*view.Field, 0, 1+len(t.columnFields)+len(t.RelationFields))
	for i := range t.columnFields {
		field := t.columnFields[i].Field
		if t.columnFields[i].Column.IsNullable {
			field.Ptr = true
		}

		if doc != nil {
			field.Description, _ = doc.ColumnDocumentation(table, t.columnFields[i].Column.Name)
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
	field.Relation = relation.Name
	field.Tags.buildRelation(spec, relation)
	field.Tag = field.Tags.Stringify()
	t.RelationFields = append(t.RelationFields, field)
	return field
}

func (t *Type) Fields(opts ...ReflectOption) []reflect.StructField {
	var unique = map[string]bool{}
	var fields []reflect.StructField
	for _, field := range t.columnFields {
		if unique[field.Name] {
			continue
		}
		unique[field.Name] = true
		fields = append(fields, field.StructField(opts...))
	}
	for _, field := range t.RelationFields {
		if unique[field.Name] {
			continue
		}
		unique[field.Name] = true
		fields = append(fields, field.StructField(opts...))
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

var defaultPackageName = "autogen"

func PkgPath(fieldName string, pkgPath string) (fieldPath string) {

	if fieldName[0] > 'Z' || fieldName[0] < 'A' {
		fieldPath = pkgPath
	}
	return fieldPath
}
