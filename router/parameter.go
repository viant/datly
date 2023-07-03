package router

import (
	"github.com/viant/datly/view"
	"reflect"
)

func BuildParameter(field reflect.StructField) (*view.Parameter, error) {
	result := &view.Parameter{}
	paramTag := view.ParseTag("datly")
	result.Name = field.Name
	result.In = &view.Location{Kind: view.Kind(paramTag.Kind), Name: paramTag.In}
	result.Schema = view.NewSchema(field.Type)
	return result, nil
}
