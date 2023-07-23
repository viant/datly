package parser

import "encoding/json"

func MergeStructs(args ...interface{}) ([]byte, error) {
	result := map[string]interface{}{}

	for _, arg := range args {
		temp := map[string]interface{}{}
		marshalled, err := json.Marshal(arg)
		if err != nil {
			return nil, err
		}
		if string(marshalled) == "null" || string(marshalled) == "" {
			continue
		}
		if err := json.Unmarshal(marshalled, &temp); err != nil {
			return nil, err
		}

		for k, v := range temp {
			if v == nil {
				continue
			}
			if _, ok := result[k]; !ok {
				result[k] = v
				continue
			}
			switch actual := v.(type) {
			case string:
				if actual != "" {
					result[k] = actual
				}
			case int:
				if actual != 0 {
					result[k] = actual
				}
			case int64:
				if actual != 0 {
					result[k] = actual
				}
			case bool:
				if actual {
					result[k] = actual
				}
			default:
				result[k] = actual
			}
		}
	}

	return json.Marshal(result)
}
