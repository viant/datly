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
		Name   string   `json:",omitempty"`
		Embed  bool     `json:",omitempty"`
		Column string   `json:",omitempty"`
		Schema *Schema  `json:",omitempty"`
		Fields []*Field `json:",omitempty"`
	}
)

func (d *Definition) Init(ctx context.Context, types Types) error {
	if err := d.initFields(ctx, types); err != nil {
		return err
	}

	d.Schema = &Schema{}
	d.Schema.setType(buildTypeFromFields(d.Fields))
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
		return fmt.Errorf("field definition has to have schema or defined other fields")
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
		rType, err := parseType(f.Schema.DataType)
		if err != nil {
			return err
		}
		f.Schema.setType(rType)
		return nil
	}

	if f.Schema.Name != "" {
		rType, err := types.Lookup(f.Schema.Name)
		if err != nil {
			return err
		}
		f.Schema.setType(rType)
	}

	return fmt.Errorf("field %v schema can't be empty", f.Name)
}

func (f *Field) buildSchemaFromFields() error {
	f.Schema = &Schema{}
	f.Schema.setType(buildTypeFromFields(f.Fields))

	return nil
}

func buildTypeFromFields(fields []*Field) reflect.Type {
	rFields := make([]reflect.StructField, len(fields))
	for i, field := range fields {
		var tag reflect.StructTag
		if field.Column != "" {
			//reflect.StructTag(omitEmptyTag + `sqlx:"name="` + columns[i].Name + "`"
			tag = reflect.StructTag(fmt.Sprintf(`sqlx:"name=%v"`, field.Column))
		}

		var fieldPath string
		if field.Name[0] > 'Z' || field.Name[0] < 'A' {
			fieldPath = pkgPath
		}

		rFields[i] = reflect.StructField{
			Name:      field.Name,
			PkgPath:   fieldPath,
			Type:      field.Schema.Type(),
			Tag:       tag,
			Anonymous: field.Embed,
		}
	}

	of := reflect.StructOf(rFields)
	return of
}
