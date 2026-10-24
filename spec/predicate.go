package spec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// UnmarshalJSON rejects the removed ensure spelling instead of silently
// disabling absent-input predicate application.
func (p *Predicate) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for name := range fields {
		if strings.EqualFold(strings.TrimSpace(name), "ensure") {
			return fmt.Errorf("unsupported predicate field %q; use applyWhenAbsent", name)
		}
	}
	type predicateAlias Predicate
	var decoded predicateAlias
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	for i, arg := range decoded.HandlerArgs {
		if number, ok := arg.(json.Number); ok {
			value, err := ParseHandlerArgument(string(number))
			if err != nil {
				return err
			}
			decoded.HandlerArgs[i] = value
		}
		if err := ValidateHandlerArgument(decoded.HandlerArgs[i]); err != nil {
			return err
		}
	}
	*p = Predicate(decoded)
	return nil
}
