package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

const pkgPath = "github.com/viant/datly/view"

type (
	TypeDefinition struct {
		shared.Reference `json:",omitempty"`
		Name             string   `json:",omitempty"`
		Alias            string   `json:",omitempty"`
		Fields           []*Field `json:",omitempty"`
		_fields          map[string]bool
		Schema           *Schema     `json:",omitempty"`
		DataType         string      `json:",omitempty"`
		Cardinality      Cardinality `json:",omitempty"`
		Package          string      `json:",omitempty"`
		Ptr              bool        `json:",omitempty"`
	}

	Field struct {
		Name        string      `json:",omitempty"`
		Embed       bool        `json:",omitempty"`
		Column      string      `json:",omitempty"`
		FromName    string      `json:",omitempty"`
		Cardinality Cardinality `json:",omitempty"`
		Schema      *Schema     `json:",omitempty"`
		Fields      []*Field    `json:",omitempty"`
		Tag         string      `json:",omitempty"`
		Ptr         bool
	}
)

func (d *TypeDefinition) AddField(field *Field) {
	if len(d._fields) == 0 {
		d._fields = map[string]bool{}
	}
	if _, ok := d._fields[field.Name]; ok {
		return
	}
	d.Fields = append(d.Fields, field)
	d._fields[field.Name] = true
}

func (d *TypeDefinition) Init(ctx context.Context, typeLookup xreflect.TypeLookupFn) error {
	if err := d.initFields(ctx, typeLookup); err != nil {
		return err
	}

	d.createSchemaIfNeeded()
	if d.Ref != "" && typeLookup != nil {
		lookup, err := typeLookup("", d.Package, d.Ref)
		if err != nil {
			return err
		}

		d.Schema = NewSchema(lookup)
		return nil
	}

	if d.Schema != nil {
		parseType, err := types.GetOrParseType(typeLookup, d.Schema.DataType)
		if err != nil {
			return err
		}

		if d.Ptr && parseType.Kind() != reflect.Ptr {
			parseType = reflect.PtrTo(parseType)
		}

		d.Schema.SetType(parseType)
	} else {
		d.Schema = &Schema{}

		schemaType := buildTypeFromFields(d.Fields)
		if d.Ptr {
			schemaType = reflect.PtrTo(schemaType)
		}

		d.Schema.SetType(schemaType)
	}

	return nil
}

func (d *TypeDefinition) initFields(ctx context.Context, typeLookup xreflect.TypeLookupFn) error {
	for _, field := range d.Fields {
		if err := field.Init(ctx, typeLookup, d); err != nil {
			return err
		}
	}

	return nil
}

func (d *TypeDefinition) Type() reflect.Type {
	return d.Schema.Type()
}

func (d *TypeDefinition) createSchemaIfNeeded() {
	if d.DataType == "" {
		return
	}

	d.Schema = &Schema{DataType: d.DataType, Cardinality: d.Cardinality}
}

func (f *Field) Init(ctx context.Context, typeLookup xreflect.TypeLookupFn, d *TypeDefinition) error {
	if err := f.initChildren(ctx, typeLookup, d); err != nil {
		return err
	}

	if err := f.initType(typeLookup); err != nil {
		return err
	}

	return nil
}

func (f *Field) initType(typeLookup xreflect.TypeLookupFn) error {
	if f.Schema == nil && len(f.Fields) == 0 {
		return fmt.Errorf("_field definition has to have schema or defined other fields")
	}

	if f.Schema != nil {
		return f.initSchemaType(typeLookup)
	}

	return f.buildSchemaFromFields()
}

func (f *Field) initChildren(ctx context.Context, types xreflect.TypeLookupFn, d *TypeDefinition) error {
	for _, field := range f.Fields {
		if err := field.Init(ctx, types, d); err != nil {
			return err
		}
	}
	return nil
}

func (f *Field) initSchemaType(typesLookup xreflect.TypeLookupFn) error {
	if f.Schema.DataType != "" {
		rType, err := types.GetOrParseType(typesLookup, f.Schema.DataType)
		if err != nil {
			return err
		}

		f.Schema.SetType(rType)
		return nil
	}

	if f.Schema.Name != "" {
		rType, err := typesLookup("", "", f.Schema.Name)
		if err != nil {
			return err
		}
		f.Schema.SetType(rType)
	}

	return fmt.Errorf("_field %v schema can't be empty", f.Name)
}

func (f *Field) buildSchemaFromFields() error {
	f.Schema = &Schema{}
	f.Schema.SetType(buildTypeFromFields(f.Fields))

	return nil
}

func buildTypeFromFields(fields []*Field) reflect.Type {
	rFields := make([]reflect.StructField, len(fields))
	for i, field := range fields {

		jsonName := field.FromName
		aTagValue := jsonTagValue(jsonName, field, field.Tag)
		if field.Column != "" && !strings.Contains(string(aTagValue), "sqlx") {
			aTagValue += fmt.Sprintf(`sqlx:"name=%v" `, field.Column)
		}

		var fieldPath string
		if field.Name[0] > 'Z' || field.Name[0] < 'A' {
			fieldPath = pkgPath
		}

		fieldType := field.Schema.Type()
		if field.Ptr {
			fieldType = reflect.PtrTo(fieldType)
		}

		if field.Cardinality == Many {
			fieldType = reflect.SliceOf(fieldType)
		}

		rFields[i] = reflect.StructField{
			Name:      field.Name,
			PkgPath:   fieldPath,
			Type:      fieldType,
			Tag:       reflect.StructTag(aTagValue),
			Anonymous: field.Embed,
		}
	}

	of := reflect.StructOf(rFields)
	return of
}

func jsonTagValue(jsonName string, field *Field, tag string) string {
	if strings.Contains(tag, "json") || jsonName == "" {
		return tag
	}

	if field.Embed {
		return ""
	}

	return fmt.Sprintf(`json:"%v" `, jsonName)
}
