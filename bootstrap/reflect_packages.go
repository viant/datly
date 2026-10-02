package bootstrap

import (
	"fmt"
	"go/token"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"github.com/viant/xunsafe"
)

// ReflectedPackages is the runtime-only authority discovered from linked Go
// packages. It intentionally reads no Go source and performs no AST work.
type ReflectedPackages struct {
	Components []*RouteSource
	Types      *typecatalog.Catalog
	Packages   []string
}

type reflectedPackageResult struct {
	components []*RouteSource
	contracts  map[reflect.Type]bool
	err        error
}

// ReflectPackages discovers every linked named type in the selected packages
// and identifies tagged xdatly.Component holders from their runtime shape.
// It is the instance-bootstrap counterpart to AST-based transcribe discovery.
func ReflectPackages(includes []string) (*ReflectedPackages, error) {
	return ReflectSelectedPackages(includes, nil)
}

// ReflectSelectedPackages discovers linked contracts in selected packages.
// Independent package reflection is bounded and its results are merged in
// package order, so errors and catalog registration stay deterministic.
func ReflectSelectedPackages(includes, excludes []string) (*ReflectedPackages, error) {
	packages := reflectedPackagePaths(includes, excludes)
	result := &ReflectedPackages{Types: typecatalog.NewCatalog(), Packages: append([]string(nil), packages...)}
	contracts := map[reflect.Type]bool{}
	results := make([]reflectedPackageResult, len(packages))
	workers := runtime.GOMAXPROCS(0)
	if workers > 8 {
		workers = 8
	}
	if workers > len(packages) {
		workers = len(packages)
	}
	if workers > 0 {
		jobs := make(chan int)
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for index := range jobs {
					results[index] = reflectPackage(packages[index])
				}
			}()
		}
		for index := range packages {
			jobs <- index
		}
		close(jobs)
		wg.Wait()
	}
	for _, current := range results {
		if current.err != nil {
			return nil, current.err
		}
		result.Components = append(result.Components, current.components...)
		for contract := range current.contracts {
			contracts[contract] = true
		}
	}
	if err := registerContractTypes(result.Types, contracts); err != nil {
		return nil, err
	}
	sort.Slice(result.Components, func(i, j int) bool {
		left, right := result.Components[i], result.Components[j]
		if left.PackagePath != right.PackagePath {
			return left.PackagePath < right.PackagePath
		}
		if left.HolderType != right.HolderType {
			return left.HolderType < right.HolderType
		}
		return left.FieldName < right.FieldName
	})
	return result, nil
}

func reflectPackage(packagePath string) (result reflectedPackageResult) {
	result.contracts = map[reflect.Type]bool{}
	for _, candidate := range xunsafe.PackageTypes(packagePath) {
		typeOf := dereference(candidate)
		if typeOf == nil || typeOf.Name() == "" || typeOf.PkgPath() != packagePath {
			continue
		}
		// Package bootstrap is also the type authority for ordinary exported
		// handlers and shared shapes that are not reachable from a component
		// input/output. Unexported function-local helper types can share a
		// runtime key, so they are deliberately excluded from package authority.
		if token.IsExported(typeOf.Name()) {
			collectContractTypes(result.contracts, typeOf)
		}
		if typeOf.Kind() != reflect.Struct {
			continue
		}
		holder := reflect.New(typeOf).Interface()
		for index := 0; index < typeOf.NumField(); index++ {
			field := typeOf.Field(index)
			componentTag, present, err := tag.ParseComponent(field.Tag)
			if err != nil {
				result.err = fmt.Errorf("reflect component %s.%s: %w", typeOf, field.Name, err)
				return result
			}
			if !present {
				continue
			}
			if err = componentTag.ValidateRoute(); err != nil {
				result.err = fmt.Errorf("reflect component %s.%s: %w", typeOf, field.Name, err)
				return result
			}
			inputField, hasInput := field.Type.FieldByName("Inout")
			if !hasInput {
				inputField, hasInput = field.Type.FieldByName("Input")
			}
			outputField, hasOutput := field.Type.FieldByName("Output")
			if !hasInput || !hasOutput {
				result.err = fmt.Errorf("reflect component-tagged field %s.%s (%s) is not xdatly.Component[I,O]", typeOf, field.Name, field.Type)
				return result
			}
			source := &RouteSource{
				HolderType: typeOf.Name(), FieldName: field.Name, PackagePath: packagePath,
				Tag: componentTag, InputType: inputField.Type.String(), OutputType: outputField.Type.String(),
				LinkedInputType: inputField.Type, LinkedOutputType: outputField.Type,
			}
			collectContractTypes(result.contracts, inputField.Type)
			collectContractTypes(result.contracts, outputField.Type)
			if provider, ok := holder.(linkedHandlerProvider); ok {
				source.LinkedHandler = provider.DatlyHandler(componentTag.Handler)
			}
			result.components = append(result.components, source)
		}
	}
	return result
}

func registerContractTypes(catalog *typecatalog.Catalog, contracts map[reflect.Type]bool) error {
	types := make([]*x.Type, 0, len(contracts))
	for typeOf := range contracts {
		typeOf = dereference(typeOf)
		if typeOf == nil || typeOf.Name() == "" || typeOf.PkgPath() == "" {
			continue
		}
		types = append(types, x.NewType(typeOf))
	}
	sort.Slice(types, func(i, j int) bool { return types[i].Key() < types[j].Key() })
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage, types...); err != nil {
		return fmt.Errorf("reflect package contract types: %w", err)
	}
	return nil
}

func collectContractTypes(contracts map[reflect.Type]bool, typeOf reflect.Type) {
	typeOf = dereference(typeOf)
	if typeOf == nil {
		return
	}
	switch typeOf.Kind() {
	case reflect.Slice, reflect.Array, reflect.Pointer:
		collectContractTypes(contracts, typeOf.Elem())
	case reflect.Map:
		collectContractTypes(contracts, typeOf.Key())
		collectContractTypes(contracts, typeOf.Elem())
	case reflect.Struct:
		if typeOf.Name() != "" && typeOf.PkgPath() != "" {
			if contracts[typeOf] {
				return
			}
			contracts[typeOf] = true
		}
		for i := 0; i < typeOf.NumField(); i++ {
			collectContractTypes(contracts, typeOf.Field(i).Type)
		}
	default:
		if typeOf.Name() != "" && typeOf.PkgPath() != "" {
			contracts[typeOf] = true
		}
	}
}

func reflectedPackagePaths(includes, excludes []string) []string {
	seen := map[string]bool{}
	var patterns []string
	for _, include := range includes {
		include = strings.TrimSpace(include)
		if include == "" {
			continue
		}
		if !strings.ContainsAny(include, "*?[") && !strings.HasSuffix(include, "...") {
			seen[include] = true
			_ = xunsafe.PackageTypes(include)
		} else {
			patterns = append(patterns, include)
		}
	}
	// Exact package selections need no walk over every linked package name.
	if len(patterns) > 0 {
		_ = xunsafe.PackageTypes("")
		for _, packagePath := range xunsafe.PackageNames() {
			for _, include := range patterns {
				matched := false
				if strings.HasSuffix(include, "...") {
					matched = strings.HasPrefix(packagePath, strings.TrimSuffix(include, "..."))
				} else if value, err := path.Match(include, packagePath); err == nil {
					matched = value
				}
				if matched {
					seen[packagePath] = true
					break
				}
			}
		}
	}
	result := make([]string, 0, len(seen))
	for packagePath := range seen {
		excluded := false
		for _, pattern := range excludes {
			if reflectedPackageMatch(pattern, packagePath) {
				excluded = true
				break
			}
		}
		if !excluded {
			result = append(result, packagePath)
		}
	}
	sort.Strings(result)
	return result
}

func reflectedPackageMatch(pattern, packagePath string) bool {
	pattern = strings.TrimSpace(pattern)
	if strings.HasSuffix(pattern, "...") {
		return strings.HasPrefix(packagePath, strings.TrimSuffix(pattern, "..."))
	}
	if matched, err := path.Match(pattern, packagePath); err == nil {
		return matched
	}
	return false
}

func dereference(typeOf reflect.Type) reflect.Type {
	for typeOf != nil && typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf
}
