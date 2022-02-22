package data

import "github.com/viant/datly/shared"

//Names represents columns names slice.
type Names []string

//Index creates presence map.
//Uses shared.KeysOf
func (c Names) Index() map[string]bool {
	result := make(map[string]bool)
	for _, column := range c {
		keys := shared.KeysOf(column, true)
		for _, key := range keys {
			result[key] = true
		}
	}
	return result
}
