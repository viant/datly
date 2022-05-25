package cmd

import (
	"fmt"
	"github.com/viant/datly/gateway/registry"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"reflect"
	"strings"
)

func addParameters(options *Options, route *router.Resource, aView *view.View) error {
	if len(options.Parameters) > 0 {
		var parameters = []*view.Parameter{}
		for _, param := range options.Parameters {
			if !strings.Contains(param, ":") {
				return fmt.Errorf("invalid param: %v, expected format: name:type", param)
			}
			pair := strings.SplitN(param, ":", 2)
			aParam := &view.Parameter{
				Name: pair[0],
				In: &view.Location{
					Kind: view.QueryKind,
				},
				Schema: &view.Schema{
					DataType: pair[1],
				},
			}

			switch pair[1] {
			case "int":
				registry.Types[pair[1]] = reflect.TypeOf(0)
			case "string":
				registry.Types[pair[1]] = reflect.TypeOf("")

			}
			parameters = append(parameters, aParam)
		}
		from := aView.From
		if from == "" {
			from = "SELECT * FROM " + aView.Table
		}
		aView.Template = &view.Template{Parameters: parameters, Source: from}
	}
	return nil
}
