package view

import "reflect"

func isSlice(fType reflect.Type) bool {
	if fType.Kind() == reflect.Ptr {
		fType = fType.Elem()
	}
	return fType.Kind() == reflect.Slice
}

func ensureStruct(fType reflect.Type) reflect.Type {
	switch fType.Kind() {
	case reflect.Ptr:
		return ensureStruct(fType.Elem())
	case reflect.Slice:
		return ensureStruct(fType.Elem())
	case reflect.Struct:
		return fType
	}
	return nil
}

// PackagedType represtns a package type
type PackagedType struct {
	Package string
	Name    string
	reflect.Type
}

// TypeName returns type name
func (p *PackagedType) TypeName() string {
	if p.Package == "" {
		return p.Name
	}
	return p.Package + "." + p.Name
}

func NewPackagedType(pkg string, name string, t reflect.Type) *PackagedType {
	return &PackagedType{Package: pkg, Name: name, Type: t}
}
