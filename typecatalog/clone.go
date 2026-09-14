package typecatalog

import (
	"reflect"

	x "github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

// CloneDescriptor returns a descriptor whose mutable synthetic syntax is
// detached from source. It is the shared clone boundary for catalog snapshots
// and package-aware resolvers.
func CloneDescriptor(source *x.Type) (*x.Type, error) {
	return (x.Cloner{}).Type(source)
}

func equalSyntheticType(left, right *smodel.Type) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Name == right.Name &&
		left.PkgPath == right.PkgPath &&
		left.ReflectType == right.ReflectType &&
		left.LinkedinType == right.LinkedinType &&
		reflect.DeepEqual(left.TypeSpec, right.TypeSpec) &&
		reflect.DeepEqual(left.Imports, right.Imports) &&
		reflect.DeepEqual(left.MethodImports, right.MethodImports) &&
		reflect.DeepEqual(left.MethodsAST, right.MethodsAST) &&
		reflect.DeepEqual(left.PtrMethodsAST, right.PtrMethodsAST) &&
		reflect.DeepEqual(left.Methods, right.Methods) &&
		reflect.DeepEqual(left.TypeParams, right.TypeParams)
}
