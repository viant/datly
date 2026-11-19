package session

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/tagly/choice"
	"reflect"
)

func parameterEnum(parameter *state.Parameter) (*choice.Enum, error) {
	if parameter.In == nil {
		return nil, nil
	}
	switch parameter.In.Kind {
	case state.KindHeader, state.KindQuery, state.KindPath, state.KindForm:
	default:
		return nil, nil
	}
	return choice.ParseEnum(reflect.StructTag(parameter.Tag), parameter.OutputType())
}
func decodeParameterEnum(enumeration *choice.Enum, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	var wire []byte
	var err error
	if enumeration.Accept == choice.Ordinal {
		wire = []byte(fmt.Sprint(value))
	} else {
		wire, err = json.Marshal(value)
	}
	if err != nil {
		return nil, err
	}
	data, err := enumeration.DecodeJSON(wire)
	if err != nil {
		return nil, err
	}
	target := reflect.New(enumeration.Type)
	if err = json.Unmarshal(data, target.Interface()); err != nil {
		return nil, err
	}
	return target.Elem().Interface(), nil
}

// A bare JSON body has no enclosing Go field on which the decoder can read
// choice tags. Apply its native parameter annotation before the configured
// decoder, keeping that decoder's destination type and null semantics intact.
func enumBodyLocator(parameter *state.Parameter, options *Options) (kind.Locator, error) {
	if parameter.In.Kind != state.KindRequestBody || parameter.In.Name != "" {
		return nil, nil
	}
	enumeration, err := choice.ParseEnum(reflect.StructTag(parameter.Tag), parameter.OutputType())
	if err != nil || enumeration == nil {
		return nil, err
	}
	bodyOptions := append([]locator.Option(nil), options.kindLocator.Options()...)
	decoder := locator.NewOptions(bodyOptions).UnmarshalFunc()
	bodyOptions = append(bodyOptions, locator.WithUnmarshal(func(data []byte, dest interface{}) error {
		converted, err := enumeration.DecodeJSON(data)
		if err != nil {
			return err
		}
		return decoder(converted, dest)
	}))
	return locator.NewBody(bodyOptions...)
}
