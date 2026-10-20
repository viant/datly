// Package goliteral renders literals used by native Go emitters.
package goliteral

import "strconv"

// StructTag prefers a readable raw literal without changing the tag's bytes.
// Embedded backticks and characters unsafe for raw literals require quoting.
func StructTag(tag string) string {
	if strconv.CanBackquote(tag) {
		return "`" + tag + "`"
	}
	return strconv.Quote(tag)
}
