package codegen

import "strings"

type Imports struct {
	Types        []string
	typeIndex    map[string]bool
	Packages     []string
	packageIndex map[string]bool
}

func (i *Imports) AddType(typeName string) {
	_, ok := i.typeIndex[typeName]
	if ok {
		return
	}
	i.typeIndex[typeName] = true
	i.Types = append(i.Types, typeName)
}

func (i *Imports) AddPackage(pkg string) {
	_, ok := i.packageIndex[pkg]
	if ok {
		return
	}
	i.packageIndex[pkg] = true
	i.Packages = append(i.Packages, pkg)
}

func (i *Imports) TypeImports() string {
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

func NewImports() *Imports {
	return &Imports{
		Types:        nil,
		typeIndex:    map[string]bool{},
		Packages:     nil,
		packageIndex: map[string]bool{},
	}
}
