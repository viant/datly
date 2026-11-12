package parity

import (
	"fmt"
	"reflect"
	"sort"
)

// Diff compares two canonical maps and returns human-readable mismatches.
func Diff(a, b map[string]any) []string {
	var issues []string
	diffValue("$", a, b, &issues)
	sort.Strings(issues)
	return issues
}

func diffValue(path string, a, b any, issues *[]string) {
	if a == nil && b == nil {
		return
	}
	if a == nil || b == nil {
		*issues = append(*issues, fmt.Sprintf("%s: one side is nil", path))
		return
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		*issues = append(*issues, fmt.Sprintf("%s: type mismatch %T != %T", path, a, b))
		return
	}
	switch av := a.(type) {
	case map[string]any:
		bv := b.(map[string]any)
		for k, v := range av {
			bvItem, ok := bv[k]
			if !ok {
				*issues = append(*issues, fmt.Sprintf("%s.%s: missing in rhs", path, k))
				continue
			}
			diffValue(path+"."+k, v, bvItem, issues)
		}
		for k := range bv {
			if _, ok := av[k]; !ok {
				*issues = append(*issues, fmt.Sprintf("%s.%s: missing in lhs", path, k))
			}
		}
	case []any:
		bv := b.([]any)
		if len(av) != len(bv) {
			*issues = append(*issues, fmt.Sprintf("%s: len mismatch %d != %d", path, len(av), len(bv)))
			return
		}
		for i := range av {
			diffValue(fmt.Sprintf("%s[%d]", path, i), av[i], bv[i], issues)
		}
	default:
		if !reflect.DeepEqual(a, b) {
			*issues = append(*issues, fmt.Sprintf("%s: value mismatch %v != %v", path, a, b))
		}
	}
}
