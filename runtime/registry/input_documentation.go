package registry

import (
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	"reflect"
)

// WithDocumentation clones metadata only. The canonical immutable Bindly plans
// and value projection remain the same, without invoking or rewriting binding.
func (c *InputContract) WithDocumentation(snapshot *docs.Snapshot) *InputContract {
	if c == nil {
		return nil
	}
	copy := *c
	copy.routes = make(map[string]*RouteInputContract, len(c.routes))
	for key, route := range c.routes {
		item := *route
		item.contract = &copy
		item.fields = append([]InputField(nil), route.fields...)
		for i := range item.fields {
			item.fields[i].documentation = snapshot
		}
		copy.routes[key] = &item
	}
	return &copy
}
func (f InputField) Documentation() *docs.Snapshot { return f.documentation }
func (f InputField) Origin() spec.RouteRef         { return f.origin }
func (f InputField) StructField() (reflect.StructField, error) {
	return xshape.Linked(f.owner).StructField(f.path)
}
