package asset

import (
	"encoding/json"
	"github.com/viant/toolbox"
)

func EncodeJSON(any interface{}) ([]byte, error) {
	aMap := map[string]interface{}{}
	data, _ := json.Marshal(any)
	_ = json.Unmarshal(data, &aMap)
	compacted := map[string]interface{}{}
	_ = toolbox.CopyNonEmptyMapEntries(aMap, compacted)
	return json.MarshalIndent(compacted, "", "\t")
}
