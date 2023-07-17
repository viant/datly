package inference

import "strings"

type Imports struct {
	Types        []string
	typeIndex    map[string]bool
	Packages     []string
	packageIndex map[string]bool
}

func (i *Imports) Clone() *Imports {
	var result = NewImports()
	result.Types = i.Types
	result.Packages = i.Packages
	for k := range i.typeIndex {
		result.typeIndex[k] = true
	}
	for k := range i.packageIndex {
		result.packageIndex[k] = true
	}
	return &result
}
func (i *Imports) AddType(typeName string) {
	if typeName == "" {
		return
	}
	_, ok := i.typeIndex[typeName]
	if ok {
		return
	}
	i.typeIndex[typeName] = true
	i.Types = append(i.Types, typeName)
}

func (i *Imports) AddPackage(pkg string) {
	if pkg == "" {
		return
	}
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
	builder.WriteString("\nimport (")
	for _, item := range i.Types {
		builder.WriteString("\n\t\"")
		builder.WriteString(item)
		builder.WriteString("\"")
	}
	builder.WriteString("\n\t)\n")
	return builder.String()
}

func (i *Imports) PackageImports() string {
	if len(i.Packages) == 0 {
		return ""
	}
	builder := strings.Builder{}
	builder.WriteString("\nimport (")
	i.rawImports(&builder)
	builder.WriteByte(')')
	return builder.String()
}

func (i *Imports) RawImports() string {
	builder := strings.Builder{}
	i.rawImports(&builder)
	return builder.String()
}
func (i *Imports) rawImports(builder *strings.Builder) {
	for _, item := range i.Packages {
		builder.WriteString("\t\"")
		builder.WriteString(item)
		builder.WriteString("\"\n")
	}
}

func NewImports() Imports {
	return Imports{
		Types:        nil,
		typeIndex:    map[string]bool{},
		Packages:     nil,
		packageIndex: map[string]bool{},
	}
}

func (i *Imports) DefaultPackageImports() string {
	if len(i.Packages) == 0 {
		return ""
	}
	builder := strings.Builder{}
	builder.WriteString("\nimport (")
	for _, item := range i.Packages {
		builder.WriteString("\t_\t\"")
		builder.WriteString(item)
		builder.WriteString("\"\n")
	}
	builder.WriteByte(')')
	return builder.String()
}
