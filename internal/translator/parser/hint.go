package parser

import (
	"encoding/json"
	"strings"
)

func tryUnmarshalHint(hint string, any interface{}) error {
	hint = strings.TrimSpace(hint)
	if hint == "" {
		return nil
	}

	return hintToStruct(hint, any)
}

func hintToStruct(encoded string, aStructPtr interface{}) error {
	encoded = strings.ReplaceAll(encoded, "/*", "")
	encoded = strings.ReplaceAll(encoded, "*/", "")
	encoded = strings.TrimSpace(encoded)
	return json.Unmarshal([]byte(encoded), aStructPtr)
}
