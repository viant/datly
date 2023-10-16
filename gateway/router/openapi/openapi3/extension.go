package openapi3

import (
	"context"
	"encoding/json"
	"strings"
)

type Extension map[string]interface{}


func (e *Extension) UnmarshalJSON(b []byte) error {
	var aMap map[string]interface{}
	err := json.Unmarshal(b, &aMap)
	if err != nil {
		return err
	}
	e.merge(aMap)
	return nil
}

func (e *Extension) merge(aMap map[string]interface{}) {
	if len(aMap) == 0 {
		return
	}
	for k, v := range aMap {
		if strings.HasPrefix(k, "x-") {
			(*e)[k] = v
		}
	}
}

//CustomExtension represents custom extension
type CustomExtension Extension
func (p *CustomExtension) UnmarshalYAML(ctx context.Context, fn func(dest interface{}) error) error {
	tmp :=map[string]interface{}{}
	err := fn(&tmp)
	if err != nil {
		return err
	}
	*p = map[string]interface{}{}
	for k, v := range tmp {
		if strings.HasPrefix(k, "x-") {
			(*p)[k] = v
		}
	}
	return nil
}