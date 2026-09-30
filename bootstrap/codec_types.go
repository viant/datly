package bootstrap

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/tags"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xunsafe"
)

// CanonicalCodecName resolves only explicit type references. Short factory names
// are deliberately not qualified using the component package or import basenames.
func CanonicalCodecName(name string, scope *typecatalog.ResolutionContext) (string, error) {
	name = strings.TrimSpace(name)
	if !strings.Contains(name, ".") || strings.Contains(name, "/") {
		return name, nil
	}
	reference, err := (xshape.Resolver{}).Reference(name)
	if err != nil {
		return name, nil
	} // Named factories need not be Go expressions.
	matched := false
	if scope != nil {
		aliases := map[string]string{}
		for _, item := range scope.Imports {
			if item.Alias != reference.Qualifier {
				continue
			}
			matched = true
			if prior := aliases[item.Alias]; prior != "" && prior != item.Package {
				return "", fmt.Errorf("codec import alias %q is ambiguous", item.Alias)
			}
			aliases[item.Alias] = item.Package
		}
	}
	if !matched {
		return name, nil
	}
	resolver, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, scope)
	if err != nil {
		return "", err
	}
	return resolver.CanonicalDeclaration(name, "")
}

// NormalizeCodecReferences updates compiler-owned metadata and returns only
// explicitly qualified dependencies. It never constructs a codec.
func NormalizeCodecReferences(component *spec.Component, scope *typecatalog.ResolutionContext) ([]string, error) {
	return componentCodecReferences(component, scope, true)
}

func componentCodecReferences(component *spec.Component, scope *typecatalog.ResolutionContext, normalize bool) ([]string, error) {
	seen := map[string]bool{}
	add := func(codec *spec.Codec) error {
		if codec == nil {
			return nil
		}
		name, err := CanonicalCodecName(codec.Body, scope)
		if err != nil {
			return fmt.Errorf("codec %q: %w", codec.Body, err)
		}
		if normalize {
			codec.Body = name
		}
		if path, _, err := (xshape.Resolver{}).CanonicalReference(name); err == nil && path != "" {
			seen[name] = true
		}
		return nil
	}
	if component == nil {
		return nil, nil
	}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param != nil {
			if err := add(param.Codec); err != nil {
				return nil, err
			}
		}
	}
	visited := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			parsed := tags.NewTags(column.Tag)
			definition := column.Codec
			if raw := parsed.Lookup(tag.CodecName); raw != nil && definition == nil {
				codec, err := tag.ParseCodec(string(raw.Values))
				if err != nil {
					return err
				}
				if codec != nil {
					definition = &spec.Codec{Body: codec.Body, Args: codec.Arguments, OutputType: codec.OutputType}
				}
			}
			if err := add(definition); err != nil {
				return err
			}
			if normalize && definition != nil {
				column.Codec = definition
				value, err := (tag.Codec{Body: definition.Body, Arguments: definition.Args, OutputType: definition.OutputType}).Value()
				if err != nil {
					return err
				}
				parsed.Set(tag.CodecName, value)
				column.Tag = parsed.Stringify()
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := visit(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(component.RootView); err != nil {
		return nil, err
	}
	for _, view := range component.Views {
		if err := visit(view); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// LinkedCodecType follows one canonical reference without registering unrelated
// package types. Existing catalog authority is checked by its caller first.
func LinkedCodecType(name string) (reflect.Type, error) {
	path, typeName, err := (xshape.Resolver{}).CanonicalReference(name)
	if err != nil || path == "" {
		return nil, nil
	}
	var selected reflect.Type
	for _, candidate := range xunsafe.PackageTypes(path) {
		candidate = dereference(candidate)
		if candidate == nil || candidate.PkgPath() != path || candidate.Name() != typeName {
			continue
		}
		if selected != nil && selected != candidate {
			return nil, fmt.Errorf("codec type %q has ambiguous linked identities", name)
		}
		selected = candidate
	}
	return selected, nil
}

// CodecReferences includes linked contract tags, because child and summary row
// columns may not have been materialized into the spec graph yet.
func CodecReferences(component *spec.Component, scope *typecatalog.ResolutionContext, contracts ...reflect.Type) ([]string, error) {
	names, err := componentCodecReferences(component, scope, false)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
	}
	visited := map[reflect.Type]bool{}
	var visit func(reflect.Type) error
	visit = func(typ reflect.Type) error {
		for typ != nil && (typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
			typ = typ.Elem()
		}
		if typ == nil || typ.Kind() != reflect.Struct || visited[typ] {
			return nil
		}
		visited[typ] = true
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			if raw := field.Tag.Get(tag.CodecName); raw != "" {
				codec, err := tag.ParseCodec(raw)
				if err != nil {
					return err
				}
				if codec == nil {
					continue
				}
				name, err := CanonicalCodecName(codec.Body, scope)
				if err != nil {
					return err
				}
				if path, _, err := (xshape.Resolver{}).CanonicalReference(name); err == nil && path != "" {
					seen[name] = true
				}
				continue
			}
			if err := visit(field.Type); err != nil {
				return err
			}
		}
		return nil
	}
	for _, typ := range contracts {
		if err := visit(typ); err != nil {
			return nil, err
		}
	}
	names = names[:0]
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (c *artifactCompiler) codecTypeLookup(component *spec.Component, scope *typecatalog.ResolutionContext) (func(string) (reflect.Type, error), error) {
	// Codec discovery extends a private catalog, never the caller's catalog or
	// a reusable factory. This also covers child metadata discovered by prepareViews.
	names, err := CodecReferences(component, scope, c.input.InputType, c.input.OutputType)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return c.lookupType, nil
	}
	catalog := typecatalog.NewCatalog()
	if c.input.Types != nil {
		catalog, err = c.input.Types.Clone()
		if err != nil {
			return nil, err
		}
	}
	for _, name := range names {
		_, found, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, name)
		if err != nil {
			return nil, err
		}
		if found {
			continue
		}
		typ, err := LinkedCodecType(name)
		if err != nil {
			return nil, err
		}
		if typ != nil {
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
				return nil, err
			}
		}
	}
	c.input.Types = catalog
	return func(name string) (reflect.Type, error) {
		canonical, err := CanonicalCodecName(name, scope)
		if err != nil {
			return nil, err
		}
		resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, scope)
		if err != nil {
			return nil, err
		}
		descriptor, err := resolver.Descriptor(canonical)
		if err != nil {
			return nil, err
		}
		if descriptor != nil {
			if descriptor.Type == nil {
				return nil, fmt.Errorf("codec type %q is present in source but not linked into this binary", canonical)
			}
			return descriptor.Type, nil
		}
		typ, err := LinkedCodecType(canonical)
		if err != nil || typ == nil {
			return nil, err
		}
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			return nil, err
		}
		return typ, nil
	}, nil
}
