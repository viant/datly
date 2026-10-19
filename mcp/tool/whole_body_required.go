package tool

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/viant/mcp-protocol/schema"
)

// An empty body location retains the original anonymous discovery authority.
// Associate only an exact single member with its explicitly declared wire slice;
// this affects advertisement, never body reconstruction or runtime validation.
func wholeBodyMemberSchemaRequired(argument Argument) (bool, bool) {
	if argument.sourceKind != "body" || argument.sourceName != "" || argument.wireType == nil || argument.wireSchema != nil || len(argument.wireSchemas) != 0 {
		return false, false
	}
	source, destination := argument.sourceType, argument.destinationType
	for source != nil && source.Kind() == reflect.Pointer {
		source = source.Elem()
	}
	for destination != nil && destination.Kind() == reflect.Pointer {
		destination = destination.Elem()
	}
	if source == nil || source != destination || source.Kind() != reflect.Struct || source.NumField() != 1 {
		return false, false
	}
	if !reflect.PointerTo(source).Implements(reflect.TypeFor[json.Unmarshaler]()) && !source.Implements(reflect.TypeFor[json.Unmarshaler]()) {
		return false, false
	}
	member := source.Field(0)
	if !member.IsExported() || member.Anonymous || member.Tag.Get("internal") == "true" || member.Tag.Get("anonymous") == "true" {
		return false, false
	}
	jsonParts := strings.Split(member.Tag.Get("json"), ",")
	for _, option := range jsonParts[1:] {
		if option == "inline" {
			return false, false
		}
	}
	projected := jsonParts[0]
	if projected == "-" {
		return false, false
	}
	if projected == "" {
		projected = member.Name
	}
	_, _, hidden, err := publicFieldName(projected, projected, member)
	if err != nil || hidden {
		return false, false
	}
	if argument.publicName != member.Name && argument.publicName != projected {
		return false, false
	}
	if member.Type.Kind() != reflect.Slice || argument.wireType.Kind() != reflect.Slice || member.Type.Elem() != argument.wireType.Elem() {
		return false, false
	}
	_, required := schema.StructToProperties(source)
	for _, name := range required {
		if name == projected {
			return true, true
		}
	}
	return false, true
}
