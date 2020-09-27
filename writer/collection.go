package writer

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/data"
	"github.com/viant/gtly"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
)

//NewCollection create a collection for data view
func NewCollection(data map[string]interface{}, view *data.View, io *data.IO) (gtly.Collection, error) {
	result := gtly.NewProvider(view.Name).NewArray()
	values, ok := data[io.Key]
	if !ok {
		if shared.IsLoggingEnabled() {
			fmt.Printf("no input data for %v\n", io.Key)
		}
		return result, nil
	}

	if view.CaseFormat != io.CaseFormat {
		if err := result.Proto().InputCaseFormat(view.CaseFormat, io.CaseFormat); err != nil {
			return nil, err
		}
	}
	switch io.Cardinality {
	case shared.CardinalityOne:
		aMap, ok := values.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("invalid input data: %v, expected: %T, but had: %T", io.Key, aMap, values)
		}

		result.Add(aMap)
	default:
		//TODO optimize storage in the original json decoding, and add optimized data type support here
		aSlice := toolbox.AsSlice(values)
		for i := range aSlice {
			result.Add(toolbox.AsMap(aSlice[i]))
		}
	}
	return result, nil
}
