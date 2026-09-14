package typecatalog

import (
	"go/token"
	"reflect"
	"strings"
	"unicode"

	"github.com/viant/tagly/format/text"
	xshape "github.com/viant/x/shape"
)

// FieldName converts an authored identifier into its canonical exported Go
// field name using Tagly's case-format authority.
func FieldName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return text.DetectCaseFormat(name).Format(name, text.CaseFormatUpperCamel)
}

// ExportedFieldName preserves an already exported Go identifier and otherwise
// applies the canonical authored-name conversion.
func ExportedFieldName(name string) string {
	name = strings.TrimSpace(name)
	if token.IsIdentifier(name) && token.IsExported(name) {
		return name
	}
	return FieldName(name)
}

// FieldByName resolves an authored field name against a Go struct type.
func FieldByName(structType reflect.Type, name string) (reflect.StructField, bool) {
	structType = (xshape.Runtime{}).Indirect(structType)
	if structType == nil || structType.Kind() != reflect.Struct {
		return reflect.StructField{}, false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return reflect.StructField{}, false
	}
	candidates := []string{name}
	runes := []rune(name)
	runes[0] = unicode.ToUpper(runes[0])
	if goName := string(runes); goName != name {
		candidates = append(candidates, goName)
	}
	goName := FieldName(name)
	if goName != "" && goName != name {
		candidates = append(candidates, goName)
	}
	return xshape.Linked(structType).FirstStructField(candidates...)
}
