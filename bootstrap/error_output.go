package bootstrap

import (
	"reflect"
	"slices"

	"github.com/viant/datly/spec"
)

type errorOutputProvider interface {
	DatlyErrorOutputs() []spec.ErrorOutput
}

func (s *RouteSource) linkedErrorOutputs() []spec.ErrorOutput {
	if s.linkedHolderType == nil {
		return nil
	}
	if holder, ok := reflect.New(s.linkedHolderType).Interface().(errorOutputProvider); ok {
		return slices.Clone(holder.DatlyErrorOutputs())
	}
	return nil
}
