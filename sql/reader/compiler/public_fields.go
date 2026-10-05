package compiler

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/runtime/output"
	"reflect"
	"strings"
)

func (b *planCompiler) compilePublicFields(views *viewSet, rootPath string) error {
	presentation, err := (output.Compiler{Lookup: b.input.TypeLookup}).Compile(output.CompileInput{Component: b.input.Component, Type: b.input.OutputType, DataField: rootPath})
	if err != nil {
		return err
	}
	visited := map[*data.View]bool{}
	var visit func(*data.View, string) error
	visit = func(view *data.View, path string) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		rowType := views.rowTypes[view]
		for rowType != nil && (rowType.Kind() == reflect.Ptr || rowType.Kind() == reflect.Slice) {
			rowType = rowType.Elem()
		}
		names, err := presentation.SelectorFieldNames(rowType, path)
		if err != nil {
			return err
		}
		view.SelectorFieldsBound = names != nil
		for _, field := range names {
			for _, column := range view.Columns {
				if column != nil && column.Name == field.GoName {
					source := column.Column
					if source == "" {
						source = column.Name
					}
					view.SelectorFields = append(view.SelectorFields, data.SelectorField{GoName: field.GoName, PublicName: field.PublicName, Column: source, Index: field.Index})
				}
			}
			for _, relation := range view.Relations {
				if relation != nil && relation.Holder == field.GoName {
					view.SelectorFields = append(view.SelectorFields, data.SelectorField{GoName: field.GoName, PublicName: field.PublicName, Holder: true, Index: field.Index})
				}
			}
		}
		for _, relation := range view.Relations {
			if relation == nil || relation.Of == nil {
				continue
			}
			childPath := relation.Holder
			if path != "" && !relation.IsOutput() {
				childPath = path + "." + childPath
			}
			if err := visit(relation.Of.View, strings.Trim(childPath, ".")); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(views.root, rootPath)
}
