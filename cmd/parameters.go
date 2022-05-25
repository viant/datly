package cmd

import (
	"fmt"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"strings"
)

type paramLiteral string

func (p paramLiteral) Name() string {
	return strings.Split(string(p), ":")[0]
}
func (p paramLiteral) Schema() *view.Schema {
	parts := strings.Split(string(p), ":")
	if len(parts) == 1 {
		return &view.Schema{DataType: "string"}
	}
	return &view.Schema{
		DataType: parts[1],
	}
}

func (p paramLiteral) In() *view.Location {
	parts := strings.Split(string(p), ":")
	if len(parts) <= 2 {
		return &view.Location{Kind: view.QueryKind, Name: p.Name()}
	}
	return &view.Location{Kind: view.Kind(parts[2])}
}

func addParameters(options *Options, route *router.Resource, aView *view.View) error {
	if len(options.Parameters) == 0 {
		return nil
	}
	var SQLSuffix = ""
	var parameters = []*view.Parameter{}
	for i := range options.Parameters {
		item := paramLiteral(options.Parameters[i])
		paramName := strings.Title(item.Name())
		aParam := &view.Parameter{
			Name:   paramName,
			In:     item.In(),
			Schema: item.Schema(),
		}
		parameters = append(parameters, aParam)
		SQLSuffix += fmt.Sprintf(`#if $Has.%v{
-- $%v			
}`, paramName, paramName)
	}

	from := aView.From
	if from == "" {
		from = "SELECT * FROM " + aView.Table
	}
	aView.Template = &view.Template{Parameters: parameters, Source: from + "\n" + SQLSuffix}

	return nil
}
