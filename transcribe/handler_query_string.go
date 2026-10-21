package transcribe

import "github.com/viant/datly/spec"

// backedScalarWireSplit requires resolved pair proof and persisted source authority.
func backedScalarWireSplit(authored, actual *spec.Parameter, destinationMatches bool) bool {
	if !destinationMatches || authored == nil || actual == nil {
		return false
	}
	switch authored.Source.Kind {
	case "query", "form", "path":
	default:
		return false
	}
	return actual.TypeExpr != "" && actual.Source.Kind == authored.Source.Kind && authored.Codec == nil && actual.Codec == nil && authored.WireTypeExpr == "" && actual.WireTypeExpr == "" && authored.WireSchema == nil && actual.WireSchema == nil && len(authored.WireSchemas) == 0 && len(actual.WireSchemas) == 0
}
