package binder

import (
	"github.com/viant/datly/data"
	"github.com/viant/toolbox"
	"strings"
)

//bindingData represents data bindingData
type bindingData struct {
	uniques map[string]map[string]bool
	data    map[string][]string
}

//Add adds bindingData data
func (b *bindingData) Add(record data.Record) {
	if len(b.data) == 0 {
		b.data = make(map[string][]string)
		b.uniques = make(map[string]map[string]bool)
	}
	for k, v := range record {
		if v == nil {
			continue
		}
		if _, ok := b.data[k]; !ok {
			b.data[k] = make([]string, 0)
			b.uniques[k] = make(map[string]bool)
		}
		textVal, ok := v.(string)
		if ok {
			textVal = "'" + textVal + "'"
		} else {
			textVal = toolbox.AsString(v)
		}
		if b.uniques[k][textVal] {
			continue
		}
		b.uniques[k][textVal] = true
		b.data[k] = append(b.data[k], textVal)
	}
}

//Data returns bindingData data
func (b *bindingData) Data() map[string]string {
	var result = make(map[string]string)
	if len(b.data) == 0 {
		return result
	}
	for k, v := range b.data {
		result[k] = strings.Join(v, ",")
	}
	return result
}

func newBindingData(columns []string) *bindingData {
	result := &bindingData{
		uniques: make(map[string]map[string]bool),
		data:    make(map[string][]string),
	}
	if len(columns) == 0 {
		return result
	}
	for _, column := range columns {
		result.uniques[column] = make(map[string]bool)
		result.data[column] = make([]string, 0)
	}
	return result
}
