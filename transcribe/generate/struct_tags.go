package generate

import (
	"sort"

	"github.com/viant/tagly/tags"
)

// generatedStructTag orders outer keys only at emission, after field rewrites.
// Stable sorting retains duplicate keys for the existing validation owners;
// Literal preserves empty values and does not reorder options within a value.
func generatedStructTag(raw string) string {
	parsed, err := tags.Parse(raw)
	if err != nil {
		// Do not turn a malformed tag into a valid prefix and hide the error.
		return raw
	}
	sort.SliceStable(parsed, func(i, j int) bool { return parsed[i].Name < parsed[j].Name })
	return parsed.Literal()
}
