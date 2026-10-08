package compiler

import (
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
)

func compileResolutionGroup(param *spec.Parameter, tagged bindly.BindingSpec, hasTag bool) (*bindly.ResolutionGroupSpec, error) {
	var result *bindly.ResolutionGroupSpec
	if param.ResolutionGroup != nil {
		group := param.ResolutionGroup
		result = &bindly.ResolutionGroupSpec{Name: group.Name, After: append([]string(nil), group.After...), DependsOn: append([]string(nil), group.DependsOn...)}
	}
	if hasTag && (result != nil || tagged.ResolutionGroup != nil) && !reflect.DeepEqual(result, tagged.ResolutionGroup) {
		return nil, fmt.Errorf("parameter %s resolution group disagrees with generated binding tag", param.Name)
	}
	if result != nil && (param.Activation != nil || param.When != "" || param.Async || param.Cacheable != nil || param.Codec != nil || param.EmitOutput) {
		return nil, fmt.Errorf("parameter %s has incompatible resolution group metadata", param.Name)
	}
	return result, nil
}
