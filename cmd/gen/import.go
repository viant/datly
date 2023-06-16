package gen

import "strings"

type Imports struct {
	Types    []string
	Packages []string
}

func (i Imports) TypeImports() string {
	if len(i.Types) == 0 {
		return ""
	}
	builder := strings.Builder{}
	builder.WriteString("\bimport (")
	for _, item := range i.Types {
		builder.WriteString("\t\"")
		builder.WriteString(item)
		builder.WriteString("\"\n")
	}
	builder.WriteByte(')')
	return builder.String()
}
