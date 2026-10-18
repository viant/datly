package report

import (
	"bytes"
	"encoding/json"
)

// UnmarshalSelectorJSON decodes one generated bool-selector section using its
// static SQL-to-JSON aliases. The target must be a method-free section alias.
// Ordered keys and raw values preserve encoding/json duplicate and null rules.
// Dynamic reflect-created facades do not acquire these generated methods.
func UnmarshalSelectorJSON(data []byte, target any, aliases map[string]string) error {
	var raw json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' || len(aliases) == 0 {
		return json.Unmarshal(data, target)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if _, err := decoder.Token(); err != nil {
		return err
	}
	var rewritten bytes.Buffer
	rewritten.WriteByte('{')
	first := true
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key := token.(string) // The complete input was validated as a JSON object.
		if canonical, ok := aliases[key]; ok {
			key = canonical
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
		if !first {
			rewritten.WriteByte(',')
		}
		first = false
		name, _ := json.Marshal(key)
		rewritten.Write(name)
		rewritten.WriteByte(':')
		rewritten.Write(value)
	}
	rewritten.WriteByte('}')
	return json.Unmarshal(rewritten.Bytes(), target)
}
