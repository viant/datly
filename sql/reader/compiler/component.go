package compiler

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format/text"
)

type viewSet struct {
	root     *data.View
	rowTypes map[*data.View]reflect.Type
}

func buildDataViews(component *spec.Component, outputType reflect.Type, directViewField string) (*viewSet, error) {
	return buildDataViewsWithType(component, outputType, directViewField, nil)
}

func buildDirectDataViews(component *spec.Component, directType reflect.Type) (*viewSet, error) {
	return buildDataViewsWithType(component, nil, "", directType)
}

func buildDataViewsWithType(component *spec.Component, outputType reflect.Type, directViewField string, directType reflect.Type) (*viewSet, error) {
	view := data.FromComponent(component)
	if view == nil {
		view = &data.View{Relations: []*data.Relation{}}
	}
	if view.Relations == nil {
		view.Relations = []*data.Relation{}
	}
	if component != nil && component.Settings != nil {
		if caseFormat := text.NewCaseFormat(component.Settings.CaseFormat); caseFormat != text.CaseFormatUndefined {
			view.CaseFormat = caseFormat
		}
	}
	result := &viewSet{root: view, rowTypes: map[*data.View]reflect.Type{}}
	rowRelations, outputRelations := splitOutputRelations(view.Relations)
	view.Relations = rowRelations
	deriver := newViewDeriver(result.rowTypes)
	if directType != nil {
		elemType, err := directViewElemType(directType)
		if err != nil {
			return nil, fmt.Errorf("direct view type %s: %w", directType, err)
		}
		typedColumns, err := columnsFromType(elemType)
		if err != nil {
			return nil, fmt.Errorf("build view columns: %w", err)
		}
		view.Columns = reconcileColumns(typedColumns, view.Columns)
		if err := deriver.enrich(view, elemType); err != nil {
			return nil, err
		}
	} else if directViewField != "" {
		if outputType == nil {
			return nil, fmt.Errorf("output type is required for direct view field %s", directViewField)
		}
		field, found := typecatalog.FieldByName(outputType, directViewField)
		if !found {
			return nil, fmt.Errorf("direct view field %s was not found on %s", directViewField, outputType)
		}
		elemType, err := directViewElemType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("direct view field %s: %w", field.Name, err)
		}
		typedColumns, err := columnsFromType(elemType)
		if err != nil {
			return nil, fmt.Errorf("build view columns: %w", err)
		}
		view.Columns = reconcileColumns(typedColumns, view.Columns)
		if err := deriver.enrich(view, elemType); err != nil {
			return nil, err
		}
	}
	if err := deriver.enrichOutputRelations(outputRelations, outputType); err != nil {
		return nil, err
	}
	view.Relations = append(view.Relations, outputRelations...)
	return result, nil
}

func splitOutputRelations(relations []*data.Relation) (rowRelations, outputRelations []*data.Relation) {
	for _, relation := range relations {
		if relation != nil && relation.IsOutput() {
			outputRelations = append(outputRelations, relation)
			continue
		}
		rowRelations = append(rowRelations, relation)
	}
	return rowRelations, outputRelations
}

func directViewElemType(viewType reflect.Type) (reflect.Type, error) {
	switch viewType.Kind() {
	case reflect.Slice:
		elemType := viewType.Elem()
		if elemType.Kind() == reflect.Ptr {
			elemType = elemType.Elem()
		}
		if elemType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("unsupported slice destination element kind %s", elemType.Kind())
		}
		return elemType, nil
	case reflect.Struct:
		return viewType, nil
	case reflect.Ptr:
		if viewType.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("unsupported pointer destination element kind %s", viewType.Elem().Kind())
		}
		return viewType.Elem(), nil
	default:
		return nil, fmt.Errorf("unsupported destination kind %s", viewType.Kind())
	}
}
