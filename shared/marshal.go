package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
)

func UnmarshalWithExt(data []byte, into interface{}, ext string) error {
	data = bytes.TrimSpace(data)
	data = bytes.TrimSpace(bytes.Trim(data, "/*"))
	if len(data) == 0 {
		return nil
	}

	switch ext {
	case ".yaml", ".yml":
		err := yaml.Unmarshal(data, into)
		if err != nil {
			return fmt.Errorf("failed to parse yaml due to the: %w", err)
		}
		return err
	default:
		err := json.Unmarshal(data, into)
		if err != nil {
			return fmt.Errorf("failed to parse json due to the: %w", err)
		}
		return err
	}
}
