package shared

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
)

func UnmarshalWithExt(data []byte, into interface{}, ext string) error {
	switch ext {
	case ".yaml", ".yml":
		return yaml.Unmarshal(data, into)
	default:
		return json.Unmarshal(data, into)
	}
}
