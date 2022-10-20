package view

import (
	"context"
	"fmt"
	"reflect"
)

const pkgPath = "github.com/viant/datly/view"

type (
	Definition struct {
		Name   string `json:",omitempty"`
		Fields []*Field
		*Schema
	}

	Field struct {
		Name        string      `json:",omitempty"`
		Embed       bool        `json:",omitempty"`
		Column      string      `json:",omitempty"`
		FromName    string      `json:",omitempty"`
		Cardinality Cardinality `json:",omitempty"`
		Schema      *Schema     `json:",omitempty"`
		Fields      []*Field    `json:",omitempty"`
		Tag         string
	}
)

func (d *Definition) Init(ctx context.Context, types Types) error {
	if err := d.initFields(ctx, types); err != nil {
		return err
	}

	if d.Schema != nil {
		parseType, err := GetOrParseType(map[string]reflect.Type{}, d.Schema.DataType)
		if err != nil {
			return err
		}

		d.Schema.SetType(parseType)
	} else {
		d.Schema = &Schema{}
		d.Schema.SetType(buildTypeFromFields(d.Fields))
	}

	return nil
}

func (d *Definition) initFields(ctx context.Context, types Types) error {
	for _, field := range d.Fields {
		if err := field.Init(ctx, types); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) Init(ctx context.Context, types Types) error {
	if err := f.initChildren(ctx, types); err != nil {
		return err
	}

	if err := f.initType(types); err != nil {
		return err
	}

	return nil
}

func (f *Field) initType(types Types) error {
	if f.Schema == nil && len(f.Fields) == 0 {
		return fmt.Errorf("_field definition has to have schema or defined other fields")
	}

	if f.Schema != nil {
		return f.initSchemaType(types)
	}

	return f.buildSchemaFromFields()
}

func (f *Field) initChildren(ctx context.Context, types Types) error {
	for _, field := range f.Fields {
		if err := field.Init(ctx, types); err != nil {
			return err
		}
	}
	return nil
}

func (f *Field) initSchemaType(types Types) error {
	if f.Schema.DataType != "" {
		rType, err := ParseType(f.Schema.DataType)
		if err != nil {
			return err
		}

		f.Schema.SetType(rType)
		return nil
	}

	if f.Schema.Name != "" {
		rType, err := types.Lookup(f.Schema.Name)
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
		var tag reflect.StructTag

		jsonName := field.Name
		if field.FromName != "" {
			jsonName = field.FromName
		}

		aTagValue := jsonTagValue(jsonName, field)
		if field.Column != "" {
			aTagValue += fmt.Sprintf(`sqlx:"name=%v" `, field.Column)
		}

		if field.Tag != "" {
			aTagValue += " " + field.Tag
		}

		tag = reflect.StructTag(aTagValue)
		var fieldPath string
		if field.Name[0] > 'Z' || field.Name[0] < 'A' {
			fieldPath = pkgPath
		}

		fieldType := field.Schema.Type()
		if field.Cardinality == Many {
			fieldType = reflect.SliceOf(fieldType)
		}

		rFields[i] = reflect.StructField{
			Name:      field.Name,
			PkgPath:   fieldPath,
			Type:      fieldType,
			Tag:       tag,
			Anonymous: field.Embed,
		}

	}

	of := reflect.StructOf(rFields)
	return of
}

func jsonTagValue(jsonName string, field *Field) string {
	if field.Embed {
		return ""
	}

	return fmt.Sprintf(`json:"%v" `, jsonName)
}
