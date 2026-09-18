package bootstrap

import (
	"fmt"
	"path"
	"reflect"
	"sort"
	"strings"

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

// ReflectPackages discovers every linked named type in the selected packages
// and identifies tagged xdatly.Component holders from their runtime shape.
// It is the instance-bootstrap counterpart to AST-based transcribe discovery.
func ReflectPackages(includes []string) (*ReflectedPackages, error) {
	packages := reflectedPackagePaths(includes)
	result := &ReflectedPackages{Types: typecatalog.NewCatalog(), Packages: append([]string(nil), packages...)}
	for _, packagePath := range packages {
		for _, candidate := range xunsafe.PackageTypes(packagePath) {
			typeOf := dereference(candidate)
			if typeOf == nil || typeOf.Name() == "" || typeOf.PkgPath() != packagePath {
				continue
			}
			if err := result.Types.Register(typecatalog.TypeOriginPackage, x.NewType(typeOf)); err != nil {
				return nil, fmt.Errorf("reflect package type %s.%s: %w", packagePath, typeOf.Name(), err)
			}
			if typeOf.Kind() != reflect.Struct {
				continue
			}
			holder := reflect.New(typeOf).Interface()
			for index := 0; index < typeOf.NumField(); index++ {
				field := typeOf.Field(index)
				componentTag, present, err := tag.ParseComponent(field.Tag)
				if err != nil {
					return nil, fmt.Errorf("reflect component %s.%s: %w", typeOf, field.Name, err)
				}
				if !present {
					continue
				}
				if err = componentTag.ValidateRoute(); err != nil {
					return nil, fmt.Errorf("reflect component %s.%s: %w", typeOf, field.Name, err)
				}
				inputField, hasInput := field.Type.FieldByName("Inout")
				if !hasInput {
					inputField, hasInput = field.Type.FieldByName("Input")
				}
				outputField, hasOutput := field.Type.FieldByName("Output")
				if !hasInput || !hasOutput {
					return nil, fmt.Errorf("reflect component-tagged field %s.%s (%s) is not xdatly.Component[I,O]", typeOf, field.Name, field.Type)
				}
				source := &RouteSource{
					HolderType: typeOf.Name(), FieldName: field.Name, PackagePath: packagePath,
					Tag: componentTag, InputType: inputField.Type.String(), OutputType: outputField.Type.String(),
					LinkedInputType: inputField.Type, LinkedOutputType: outputField.Type,
				}
				if provider, ok := holder.(linkedHandlerProvider); ok {
					source.LinkedHandler = provider.DatlyHandler(componentTag.Handler)
				}
				result.Components = append(result.Components, source)
			}
		}
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

func reflectedPackagePaths(includes []string) []string {
	seen := map[string]bool{}
	for _, include := range includes {
		include = strings.TrimSpace(include)
		if include != "" && !strings.ContainsAny(include, "*?") && !strings.HasSuffix(include, "...") {
			seen[include] = true
			_ = xunsafe.PackageTypes(include)
		}
	}
	_ = xunsafe.PackageTypes("")
	for _, packagePath := range xunsafe.PackageNames() {
		for _, include := range includes {
			include = strings.TrimSpace(include)
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
	result := make([]string, 0, len(seen))
	for packagePath := range seen {
		result = append(result, packagePath)
	}
	sort.Strings(result)
	return result
}

func dereference(typeOf reflect.Type) reflect.Type {
	for typeOf != nil && typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	return typeOf
}
