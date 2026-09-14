package spec

import (
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
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*p = Predicate(decoded)
	return nil
}
