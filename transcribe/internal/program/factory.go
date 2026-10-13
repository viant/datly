package program

import (
	"fmt"
	"go/token"
	"strings"

	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
)

// Symbol resolves an exported, package-qualified handler symbol.
func Symbol(ref string) (string, string, error) {
	i := strings.LastIndex(ref, ".")
	if i <= strings.LastIndex(ref, "/") || i < 1 || !token.IsIdentifier(ref[i+1:]) || !token.IsExported(ref[i+1:]) {
		return "", "", fmt.Errorf("handler symbol %q must be an exported, package-qualified name", ref)
	}
	return ref[:i], ref[i+1:], nil
}

// FactoryContracts identifies a declarative write factory whose contracts belong to its generated destination.
func FactoryContracts(header *dql.HandlerHeader, prepared *dql.PreparedSource) ([3]string, bool) {
	var names [3]string
	if header == nil || !header.Declarative || (!strings.EqualFold(header.Method, "POST") && !strings.EqualFold(header.Method, "PATCH") && !strings.EqualFold(header.Method, "PUT") && !strings.EqualFold(header.Method, "DELETE")) || prepared == nil || prepared.TypeContext == nil || prepared.TypeContext.PackagePath == "" {
		return names, false
	}
	destination := prepared.TypeContext.PackagePath
	for i, ref := range []string{header.Factory, header.InputType, header.OutputType} {
		canonical, err := (&gen.Plan{Imports: prepared.TypeContext.Imports}).CanonicalType(destination, ref)
		if err != nil {
			return names, false
		}
		pkg, name, err := Symbol(canonical)
		if err != nil || pkg != destination {
			return names, false
		}
		names[i] = name
	}

	return names, true
}
