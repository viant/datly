package spec

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Cardinality string

const (
	CardinalityOne  Cardinality = "one"
	CardinalityMany Cardinality = "many"
)

// NormalizeCardinality accepts the canonical values and the title-case values
// emitted by the original Datly model. Resolved metadata always uses the
// canonical lower-case spelling.
func NormalizeCardinality(value Cardinality) (Cardinality, error) {
	switch {
	case strings.TrimSpace(string(value)) == "":
		return "", nil
	case strings.EqualFold(strings.TrimSpace(string(value)), string(CardinalityOne)):
		return CardinalityOne, nil
	case strings.EqualFold(strings.TrimSpace(string(value)), string(CardinalityMany)):
		return CardinalityMany, nil
	default:
		return "", fmt.Errorf("unsupported cardinality %q; expected %q or %q", value, CardinalityOne, CardinalityMany)
	}
}

// UnmarshalJSON normalizes legacy title-case cardinality at the authored-spec
// boundary instead of letting it reach exact runtime switches.
func (c *Cardinality) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	normalized, err := NormalizeCardinality(Cardinality(value))
	if err != nil {
		return err
	}
	*c = normalized
	return nil
}

type TypeRef struct {
	Package string `json:"package,omitempty"`
	Name    string `json:"name"`
	// Pointer applies to the named element, inside a slice when Cardinality is many.
	Pointer     bool        `json:"pointer,omitempty"`
	Cardinality Cardinality `json:"cardinality,omitempty"`
	// SlicePointer distinguishes *[]T from []*T (Pointer with Cardinality many).
	SlicePointer bool `json:"slicePointer,omitempty"`
}

func (t TypeRef) IsZero() bool {
	return t.Package == "" && t.Name == "" && !t.Pointer && t.Cardinality == "" && !t.SlicePointer
}
