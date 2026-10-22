package readergraph

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	xshape "github.com/viant/x/shape"
)

// ParameterField resolves both persisted Go field identities and discovered
// logical binding names without treating a binding alias as a Go field path.
func ParameterField(fields []xshape.Field, parameter *spec.Parameter) (*xshape.Field, error) {
	var result *xshape.Field
	for i := range fields {
		field := &fields[i]
		metadata, err := dtag.ParseField(field.StructField())
		if err != nil {
			return nil, err
		}
		logical := field.Name
		if metadata.Binding != nil && strings.TrimSpace(metadata.Binding.Name) != "" {
			logical = strings.TrimSpace(metadata.Binding.Name)
		}
		if !strings.EqualFold(field.Name, parameter.Name) && !strings.EqualFold(logical, parameter.Name) {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("ambiguous reader parameter %s", parameter.Name)
		}
		result = field
	}
	return result, nil
}
