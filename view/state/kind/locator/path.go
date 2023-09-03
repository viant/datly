package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/toolbox"
)

type Path struct {
	parameters map[string]string
}

func (v *Path) Names() []string {
	var result = make([]string, 0)
	for key := range v.parameters {
		result = append(result, key)
	}
	return result
}

func (v *Path) Value(ctx context.Context, name string) (interface{}, bool, error) {
	ret, ok := v.parameters[name]
	return ret, ok, nil
}

// NewPath returns path locator
func NewPath(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.request == nil {
		return nil, fmt.Errorf("request was empty")
	}
	if options.URIPattern == "" {
		return nil, fmt.Errorf("uri pattern was empty")
	}
	parameters, _ := toolbox.ExtractURIParameters(options.URIPattern, options.request.URL.Path)
	ret := &Path{parameters: parameters}
	return ret, nil
}
