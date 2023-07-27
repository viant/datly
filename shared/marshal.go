package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox"
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
		transient := map[string]interface{}{}
		if err := yaml.Unmarshal(data, &transient); err != nil {
			return err
		}

		aMap := map[string]interface{}{}
		if err := yaml.Unmarshal(data, &aMap); err != nil {
			return err
		}

		if err := toolbox.DefaultConverter.AssignConverted(into, aMap); err != nil {
			return err
		}

		return nil
	default:
		err := json.Unmarshal(data, into)
		if err != nil {
			return fmt.Errorf("failed to parse json due to the: %w", err)
		}
		return err
	}
}
