package spec

import (
	"encoding/json"
	"strings"
)

// IsDerivedOutput recognizes canonical derived output and the original DQL/
// package spelling at the metadata input boundary. Generators emit "derived".
func (p *Parameter) IsDerivedOutput() bool {
	if p == nil || !strings.EqualFold(strings.TrimSpace(p.Source.Kind), "output") {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(p.Source.Name)) {
	case "derived", "summary":
		return true
	default:
		return false
	}
}

// UnmarshalJSON normalizes stored original relation metadata into the current
// DerivedView model. Unrelated relation kinds retain their authored values.
func (k *RelationKind) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(value), "summary") {
		value = string(RelationKindDerived)
	}
	*k = RelationKind(value)
	return nil
}
