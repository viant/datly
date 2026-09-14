package dql

import (
	"strings"

	"github.com/viant/datly/spec"
)

// EmbeddedSQLRefs extracts typed resource references from one authored SQL
// source. Resource expansion remains a later transcribe load concern.
func EmbeddedSQLRefs(sql string) []*spec.EmbeddedSQLRef {
	if strings.TrimSpace(sql) == "" {
		return nil
	}
	var result []*spec.EmbeddedSQLRef
	seen := map[string]bool{}
	token := "${embed:"
	searchFrom := 0
	for {
		index := strings.Index(sql[searchFrom:], token)
		if index == -1 {
			break
		}
		start := searchFrom + index
		end := strings.IndexByte(sql[start:], '}')
		if end == -1 {
			break
		}
		raw := sql[start : start+end+1]
		path := strings.TrimSpace(raw[len("${embed:") : len(raw)-1])
		if !seen[raw] {
			seen[raw] = true
			result = append(result, &spec.EmbeddedSQLRef{Path: path, Raw: raw})
		}
		searchFrom = start + end + 1
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
