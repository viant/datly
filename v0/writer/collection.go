package writer

import (
	"fmt"
	"github.com/pkg/errors"
	data2 "github.com/viant/datly/v0/data"
	shared2 "github.com/viant/datly/v0/shared"
	"github.com/viant/gtly"
	"github.com/viant/toolbox"
)

//NewCollection create a collection for view view
func NewCollection(data map[string]interface{}, view *data2.View, io *data2.IO) (gtly.Collection, error) {
	result := gtly.NewProvider(view.Name).NewArray()
	values, ok := data[io.Key]
	if !ok {
		if shared2.IsLoggingEnabled() {
			fmt.Printf("no input view for %v\n", io.Key)
		}
		return result, nil
	}

	if view.CaseFormat != io.CaseFormat {
		if err := result.Proto().InputCaseFormat(view.CaseFormat, io.CaseFormat); err != nil {
			return nil, err
		}
	}
	switch io.Cardinality {
	case shared2.CardinalityOne:
		aMap, ok := values.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("invalid input view: %v, expected: %T, but had: %T", io.Key, aMap, values)
		}

		result.Add(aMap)
	default:
		//TODO optimize storage in the original json decoding, and add optimized view type support here
		aSlice := toolbox.AsSlice(values)
		for i := range aSlice {
			result.Add(toolbox.AsMap(aSlice[i]))
		}
	}
	return result, nil
}
