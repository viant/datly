package generate

import "fmt"

// GenerationPolicy controls how persistent generated artifacts are reconciled.
// Merge preserves the existing structural shape-merge behavior. Overwrite
// replaces manifest-owned generated files from the new generator proposal after
// fingerprint validation, without parsing the previous generated source.
type GenerationPolicy string

const (
	GenerationPolicyMerge     GenerationPolicy = ""
	GenerationPolicyOverwrite GenerationPolicy = "overwrite"
)

func (p GenerationPolicy) normalize() (GenerationPolicy, error) {
	switch p {
	case "", "merge":
		return GenerationPolicyMerge, nil
	case GenerationPolicyOverwrite:
		return GenerationPolicyOverwrite, nil
	default:
		return "", fmt.Errorf("unsupported generation policy %q", p)
	}
}
