package compiler

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/output"
	sqlxio "github.com/viant/sqlx/io"
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
					alternatives, verified := selectorColumnAliases(rowType, field.Index, source)
					view.SelectorFields = append(view.SelectorFields, data.SelectorField{GoName: field.GoName, PublicName: field.PublicName, Column: source, ColumnAliases: alternatives, VerifiedColumnAliases: verified, Index: field.Index})
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

// Read alternatives from the actual output-visible field once, during compilation.
// They resolve the public field to a projected SQL result without changing its
// primary SQLX mapping or admitting the alternatives as public field names.
func selectorColumnAliases(rowType reflect.Type, index []int, primary string) ([]string, []string) {
	if rowType == nil || len(index) == 0 {
		return nil, nil
	}
	metadata := sqlxio.ParseTag(rowType.FieldByIndex(index).Tag)
	if metadata == nil {
		return nil, nil
	}
	seen := map[string]bool{primary: true}
	var result, verified []string
	for _, name := range strings.Split(metadata.Column, "|") {
		name = strings.TrimSpace(name)
		if name != "" && !seen[name] {
			seen[name] = true
			result = append(result, name)
			matched, err := sqlxio.NewMatcher(nil).Match(rowType, sqlxio.NamesToColumns([]string{name}))
			if err == nil && len(matched) == 1 && reflect.DeepEqual(matched[0].FieldIndex(), index) {
				verified = append(verified, name)
			}
		}
	}
	return result, verified
}
