package dql

import "strings"

// isCubeSetting recognizes the original Datly #set cube configuration syntax
// through the canonical directive parser, independently of parameter declarations.
func isCubeSetting(block directiveBlock) bool {
	if block.kind != directiveKindSet {
		return false
	}
	name, _, _, ok := parseDirectiveCall(block.body)
	return ok && (strings.EqualFold(name, "cube") || strings.EqualFold(name, "cubeCompose"))
}
