package transcribe

import (
	"github.com/viant/datly/spec"
	"strings"
)

// backedStringWireSplit admits only the explicit split retained by the input tag.
func backedStringWireSplit(authored, actual *spec.Parameter, destinationString bool) bool {
	return destinationString && authored != nil && actual != nil && (authored.Source.Kind == "query" || authored.Source.Kind == "form") && actual.Source.Kind == authored.Source.Kind && (authored.Required == nil || !*authored.Required) && (actual.Required == nil || !*actual.Required) && authored.Codec == nil && actual.Codec == nil && authored.WireTypeExpr == "" && actual.WireTypeExpr == "" && strings.TrimSpace(authored.TypeExpr) == "*string" && strings.TrimSpace(actual.TypeExpr) == "*string" && (strings.TrimSpace(authored.OutputTypeExpr) == "" || strings.TrimSpace(authored.OutputTypeExpr) == "string")
}
