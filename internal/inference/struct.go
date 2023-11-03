package inference

import (
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

type parameterStruct struct {
	name      string
	fields    map[string]*parameterStruct
	Parameter *Parameter
}

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
	if p.Parameter != nil && (p.Parameter.In.Kind != state.KindObject) {
		return reflect.StructField{Name: p.name, Type: p.Parameter.Schema.Type(), Tag: reflect.StructTag(p.Parameter.Tag), PkgPath: xreflect.PkgPath(p.Parameter.Name, p.Parameter.Schema.Package)}
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
