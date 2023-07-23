package inference

import (
	"encoding/json"
	"strings"
)

func TryUnmarshalHint(hint string, any interface{}) error {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return nil
	}
	return HintToStruct(hint, any)
}

func HintToStruct(encoded string, aStructPtr interface{}) error {
	encoded = strings.ReplaceAll(encoded, "/*", "")
	encoded = strings.ReplaceAll(encoded, "*/", "")
	encoded = strings.TrimSpace(encoded)
	return json.Unmarshal([]byte(encoded), aStructPtr)
}
