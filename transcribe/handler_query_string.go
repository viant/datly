package transcribe

import (
	"github.com/viant/datly/spec"
	"strings"
)

// backedScalarWireSplit admits only the explicit split retained by the input tag.
func backedScalarWireSplit(authored, actual *spec.Parameter, destinationMatches bool) bool {
	if !destinationMatches || authored == nil || actual == nil {
		return false
	}
	wire := strings.TrimSpace(authored.TypeExpr)
	destination := ""
	switch {
	case wire == "*string" && (authored.Source.Kind == "query" || authored.Source.Kind == "form"):
		destination = "string"
	case wire == "*bool" && authored.Source.Kind == "query":
		destination = "bool"
	default:
		return false
	}
	return actual.Source.Kind == authored.Source.Kind && (authored.Required == nil || !*authored.Required) && (actual.Required == nil || !*actual.Required) && authored.Codec == nil && actual.Codec == nil && authored.WireTypeExpr == "" && actual.WireTypeExpr == "" && strings.TrimSpace(actual.TypeExpr) == wire && (strings.TrimSpace(authored.OutputTypeExpr) == "" || strings.TrimSpace(authored.OutputTypeExpr) == destination)
}
