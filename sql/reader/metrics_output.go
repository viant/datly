package reader

import (
	"fmt"
	"reflect"

	"github.com/viant/x/shape"
	"github.com/viant/xdatly/response"
)

func (a *outputAccessors) compileMetrics(name string, output reflect.Type) error {
	if name != "" {
		field, err := shape.Linked(output).Accessor(name)
		if err != nil {
			return err
		}
		if !reflect.TypeOf(response.Metrics{}).AssignableTo(field.Type()) {
			return fmt.Errorf("metrics output %s must accept response.Metrics", name)
		}
		a.metrics = field
	}
	return nil
}
func (a *outputAccessors) writeMetrics(output any, metrics response.Metrics) error {
	if a == nil || a.metrics == nil || output == nil {
		return nil
	}
	return a.metrics.Set(output, metrics)
}
