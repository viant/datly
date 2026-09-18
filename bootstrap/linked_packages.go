package bootstrap

import (
	"embed"
	"reflect"
	"sort"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/xunsafe"
)

// Embedder is the established linked-package resource capability.
type Embedder interface {
	EmbedFS() *embed.FS
}

// EmbeddedResource identifies the namespace of an embedded package filesystem.
// The filesystem itself is obtained through Embedder, matching the established
// Datly input/component resource contract.
type EmbeddedResource interface {
	Embedder
	EmbedNamespace() string
}

type linkedHandlerProvider interface {
	DatlyHandler(string) func() (rhandler.TypedHandler, error)
}

func linkedHolder(holders []any, packagePath, holderName string) any {
	for _, holder := range holders {
		typeOf := reflect.TypeOf(holder)
		for typeOf != nil && typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf != nil && typeOf.PkgPath() == packagePath && typeOf.Name() == holderName {
			return holder
		}
	}
	for _, typeOf := range xunsafe.PackageTypes(packagePath) {
		for typeOf != nil && typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf == nil || typeOf.Name() != holderName || typeOf.PkgPath() != packagePath {
			continue
		}
		return reflect.New(typeOf).Interface()
	}
	return nil
}

// LinkedHolder returns the compiled holder matching one source declaration.
func LinkedHolder(holders []any, packagePath, holderName string) any {
	return linkedHolder(holders, packagePath, holderName)
}

// LinkedResources returns embedded resources exposed by compiled holders in a
// selected package. It performs no registration and invokes no init side effect.
func LinkedResources(holders []any, packagePath string) map[string]*embed.FS {
	result := map[string]*embed.FS{}
	candidates := append([]any(nil), holders...)
	for _, typeOf := range xunsafe.PackageTypes(packagePath) {
		for typeOf != nil && typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf != nil && typeOf.Name() != "" && typeOf.PkgPath() == packagePath {
			candidates = append(candidates, reflect.New(typeOf).Interface())
		}
	}
	for _, holder := range candidates {
		typeOf := reflect.TypeOf(holder)
		for typeOf != nil && typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf == nil || typeOf.PkgPath() != packagePath {
			continue
		}
		resource, ok := holder.(EmbeddedResource)
		if !ok || resource.EmbedFS() == nil || resource.EmbedNamespace() == "" {
			continue
		}
		result[resource.EmbedNamespace()] = resource.EmbedFS()
	}
	return result
}

// LinkedPackagePaths derives package scan roots from ordinary linked holders.
func LinkedPackagePaths(holders []any) []string {
	seen := map[string]bool{}
	for _, holder := range holders {
		typeOf := reflect.TypeOf(holder)
		for typeOf != nil && typeOf.Kind() == reflect.Pointer {
			typeOf = typeOf.Elem()
		}
		if typeOf != nil && typeOf.PkgPath() != "" {
			seen[typeOf.PkgPath()] = true
		}
	}
	result := make([]string, 0, len(seen))
	for packagePath := range seen {
		result = append(result, packagePath)
	}
	sort.Strings(result)
	return result
}
