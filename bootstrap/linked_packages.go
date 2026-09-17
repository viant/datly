package bootstrap

import (
	"embed"
	"reflect"
	"sort"
	"sync"

	rhandler "github.com/viant/datly/runtime/handler"
)

var defaultImports struct {
	sync.RWMutex
	holders []any
}

// UseDefaultImports publishes the concrete package holders selected by the
// application's user-owned link package. It does not parse or register any
// component contract; bootstrap still discovers contracts by scanning the
// configured package names.
func UseDefaultImports(holders ...any) {
	defaultImports.Lock()
	defer defaultImports.Unlock()
	seen := map[string]bool{}
	for _, holder := range defaultImports.holders {
		if key := linkedHolderKey(holder); key != "" {
			seen[key] = true
		}
	}
	for _, holder := range holders {
		key := linkedHolderKey(holder)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		defaultImports.holders = append(defaultImports.holders, holder)
	}
}

func linkedHolderKey(holder any) string {
	typeOf := reflect.TypeOf(holder)
	for typeOf != nil && typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	if typeOf == nil || typeOf.PkgPath() == "" || typeOf.Name() == "" {
		return ""
	}
	return typeOf.PkgPath() + "." + typeOf.Name()
}

// DefaultImports returns an isolated snapshot of the linked package holders.
func DefaultImports() []any {
	defaultImports.RLock()
	defer defaultImports.RUnlock()
	return append([]any(nil), defaultImports.holders...)
}

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
	for _, holder := range holders {
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
