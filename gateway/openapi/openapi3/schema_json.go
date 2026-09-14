package openapi3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
)

// Schema retains the existing object alternative while representing the actual
// OpenAPI additionalProperties boolean union. Invalid/unsupported schema keywords
// fail instead of being discarded during authored resource projection.
func (s Schema) MarshalJSON() ([]byte, error) {
	type plain Schema
	if s.AdditionalPropertiesAllowed != nil && s.AdditionalProperties != nil {
		return nil, fmt.Errorf("additionalProperties has both boolean and schema values")
	}
	data, err := json.Marshal(plain(s))
	if err != nil || s.AdditionalPropertiesAllowed == nil {
		return data, err
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	object["additionalProperties"], _ = json.Marshal(*s.AdditionalPropertiesAllowed)
	return json.Marshal(object)
}
func (s *Schema) UnmarshalJSON(data []byte) error {
	type plain Schema
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	var allowed *bool
	if value, ok := object["additionalProperties"]; ok && (bytes.Equal(bytes.TrimSpace(value), []byte("true")) || bytes.Equal(bytes.TrimSpace(value), []byte("false"))) {
		var flag bool
		_ = json.Unmarshal(value, &flag)
		allowed = &flag
		delete(object, "additionalProperties")
	}
	if value, ok := object["additionalProperties"]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return fmt.Errorf("additionalProperties must be boolean or schema")
	}
	normalized, err := json.Marshal(object)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(normalized))
	decoder.DisallowUnknownFields()
	var result plain
	if err = decoder.Decode(&result); err != nil {
		return err
	}
	*s = Schema(result)
	s.AdditionalPropertiesAllowed = allowed
	return nil
}
func (s *Schema) UnmarshalYAML(node *yaml.Node) error {
	var object map[string]any
	if err := node.Decode(&object); err != nil {
		return err
	}
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	return s.UnmarshalJSON(data)
}
func (s Schema) MarshalYAML() (any, error) {
	data, err := s.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var object map[string]any
	err = json.Unmarshal(data, &object)
	return object, err
}
