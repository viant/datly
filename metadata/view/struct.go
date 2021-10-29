package view

import (
	"fmt"
	"github.com/viant/datly/metadata"
	"github.com/viant/datly/metadata/sql"
	"github.com/viant/datly/metadata/tag"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

//FromStruct creates views from a struct
func FromStruct(name string, aType reflect.Type, viewCaseFormat format.Case) (*metadata.View, error) {
	view := &metadata.View{
		CaseFormat: format.CaseUpperCamel.String(),
	}
	err := updateView(name, aType, viewCaseFormat, view, true)
	if err != nil {
		return nil, err
	}
	return view, nil
}


func updateView(nameOrSQL string, aType reflect.Type, viewCaseFormat format.Case, view *metadata.View, isRoot bool) error {
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	if aType.Kind() == reflect.Slice {
		aType = aType.Elem()
	}
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	if aType.Kind() != reflect.Struct {
		return  fmt.Errorf("unsupported type: %s", aType.String())
	}
	if strings.Contains(nameOrSQL, " ") {
		view.Name = aType.Name()
		view.From = &metadata.From{
			Fragment: sql.Fragment{
				SQL: nameOrSQL,
			},
		}
	} else {
		view.Name = nameOrSQL
		view.Table = nameOrSQL
	}
	view.SetReflectType(aType)
	columns, err := metadata.DiscoverColumns(aType, viewCaseFormat)
	if err != nil {
		return err
	}
	view.Columns = columns
	for i := 0; i < aType.NumField(); i++ {
		field := aType.Field(i)
		if isExported := field.PkgPath == ""; !isExported {
			continue
		}
		fieldType := field.Type
		if shared.IsBaseType(fieldType) {
			continue
		}
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		aTag := tag.Parse(field.Tag.Get(tag.DatlyTag))
		if aTag.Transient || aTag.Table == "" {
			continue
		}
		switch fieldType.Kind() {
		case reflect.Slice:
			componentType := fieldType.Elem()
			if componentType.Kind() == reflect.Ptr {
				componentType = componentType.Elem()
			}
			if componentType.Kind() != reflect.Struct {
				return fmt.Errorf("unsupported ref type: %v", componentType.String())
			}
			ref, err := addReference(aTag, field.Name, componentType, viewCaseFormat)
			if err != nil {
				return err
			}
			ref.Cardinality = shared.CardinalityMany

			getter := xunsafe.NewField(field).AddrGetter()
			ref.SetGetter(func(instance interface{}) interface{} {
				ptr := xunsafe.Addr(instance)
				return getter(ptr)
			})
			view.AddRef(ref)
		case reflect.Struct:
			ref, err := addReference(aTag, field.Name, fieldType, viewCaseFormat)
			if err != nil {
				return err
			}
			getter := xunsafe.NewField(field).AddrGetter()
			ref.SetGetter(func(instance interface{}) interface{} {
				ptr := xunsafe.Addr(instance)
				return getter(ptr)
			})
			view.AddRef(ref)
		}
	}
	return view.Init(!isRoot)
}

func addReference(aTag *tag.Tag, refName string, refType reflect.Type, viewCaseFormat format.Case) (*metadata.Reference, error) {
	refView := &metadata.View{}
	err := updateView(aTag.Table, refType, viewCaseFormat, refView, false)
	if err != nil {
		return nil, fmt.Errorf("failed to updated ref %s, due to %w", refType.String(), err)
	}
	refView.Name = refName
	ref := &metadata.Reference{
		Name:        refName,
		DataView:    refView.Name,
		Cardinality: shared.CardinalityOne,
	}
	ref.SetView(refView)
	criteria := strings.Split(aTag.On, " AND ")
	for _, expr := range criteria {
		match, err := ref.MatchFromExpr(expr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ref.on: %v, due to %w", aTag.On, err)
		}
		ref.AddMatch(match)
	}
	return ref, nil
}
