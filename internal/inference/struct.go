package inference

import (
	"reflect"
	"strings"
)

type parameterStruct struct {
	name   string
	fields map[string]*parameterStruct
	*Parameter
}

/*{
	limit
	vendor.sql

}*/

func (p *parameterStruct) Add(name string, parameter *Parameter) {
	index := strings.Index(name, ".")
	holder := name
	child := ""
	if index != -1 {
		holder = name[:index]
		child = name[index+1:]
	}
	if _, ok := p.fields[holder]; !ok {
		p.fields[holder] = newParameterStruct(holder)
	}
	node := p.fields[holder]
	//p.name = holder
	if child == "" {
		node.Parameter = parameter
		return
	}
	node.Add(child, parameter)
}

func (p *parameterStruct) reflectType() reflect.Type {
	return p.structField().Type

}
func (p *parameterStruct) structField() reflect.StructField {
	if p.Parameter != nil {
		return reflect.StructField{Name: p.name, Type: p.Parameter.Schema.Type(), PkgPath: PkgPath(p.Name, defaultPackageName)}
	}
	var fields []reflect.StructField
	for _, f := range p.fields {
		fields = append(fields, f.structField())
	}
	pkgPath := ""
	if p.name != "" {
		pkgPath = PkgPath(p.name, defaultPackageName)
	}
	return reflect.StructField{Name: p.name, Type: reflect.StructOf(fields), PkgPath: pkgPath}
}

func newParameterStruct(name string) *parameterStruct {
	return &parameterStruct{fields: map[string]*parameterStruct{}, name: name}
}