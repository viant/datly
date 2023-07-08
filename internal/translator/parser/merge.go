package parser

import "encoding/json"

func mergeJsonStructs(args ...interface{}) ([]byte, error) {
	result := map[string]interface{}{}

	for _, arg := range args {
		marshalled, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}

		if string(marshalled) == "null" || string(marshalled) == "" {
			continue
		}

		if err := json.Unmarshal(marshalled, &result); err != nil {
			return nil, err
		}
	}

	return json.Marshal(result)
}
