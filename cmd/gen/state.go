package gen

import (
	_ "embed"
	"strings"
)

type State []*Parameter

func (s *State) Append(param ...*Parameter) {
	*s = append(*s, param...)
}

//go:embed tmpl/state.gox
var stateGoTemplate string

func (s State) GenerateDSQLDeclration() string {
	var result []string
	for _, param := range s {
		result = append(result, param.DsqlParameterDeclaration())
	}
	return strings.Join(result, "\n\t")
}

func (s State) GenerateGoCode(pkg string) string {
	if pkg == "" {
		pkg = "main"
	}
	if len(s) == 0 {
		return ""
	}
	var output = strings.Replace(stateGoTemplate, "$Package", pkg, 1)

	var fields = []string{}
	for _, input := range s {
		fields = append(fields, input.FieldDeclaration())
	}
	output = strings.Replace(output, "$Fields", strings.Join(fields, "\n\n"), 1)
	return output
}
