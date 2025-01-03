package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/toolbox"
)

type Path struct {
	path       string
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
	if name == "" {
		return v.path, true, nil
	}
	ret, ok := v.parameters[name]
	return ret, ok, nil
}

// NewPath returns path locator
func NewPath(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	URL := ""
	if len(options.Path) > 0 {
		if options.request != nil {
			URL = options.request.URL.Path
		}
		return &Path{parameters: options.Path, path: URL}, nil
	}
	if options.request == nil {
		return nil, fmt.Errorf("requestState was empty")
	}
	if options.URIPattern == "" {
		return nil, fmt.Errorf("uri pattern was empty")
	}
	parameters, _ := toolbox.ExtractURIParameters(options.URIPattern, options.request.URL.Path)
	ret := &Path{parameters: parameters, path: options.request.URL.Path}
	return ret, nil
}
