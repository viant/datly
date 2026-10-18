// Package selectorcompanion validates declaration-only selector metadata.
// It never invokes a holder method or reads its body.
package selectorcompanion

import (
	"fmt"
	"go/ast"
	"go/token"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// Method is the single optional declaration-only holder method.
const Method = "DatlyQuerySelectors"

// Contract is a checked method-result shape for one exact holder field.
// Parameters remain the canonical declarations; this is not another registry.
type Contract struct {
	Holder, Field string
	Descriptor    *x.Type
	Fields        []xshape.Field
}

// Resolve accepts a value result only. A reflected result is inspected as a type,
// never invoked. Source descriptors use the same canonical method signature.
func Resolve(holder *x.Type, fieldName string, lookup xshape.Lookup) (*Contract, error) {
	methods, err := xshape.New(holder, lookup).Methods(true)
	if err != nil {
		return nil, err
	}
	var selected *xshape.Method
	for i := range methods {
		if methods[i].Name == Method {
			if selected != nil {
				return nil, fmt.Errorf("duplicate selector companion method")
			}
			selected = &methods[i]
		}
	}
	if selected == nil {
		return nil, nil
	}
	if len(selected.Parameters) != 0 || selected.Variadic || len(selected.Results) != 1 {
		return nil, fmt.Errorf("selector companion requires no arguments and one value result")
	}
	if !concrete(holder) {
		return nil, fmt.Errorf("selector companion holder must be a concrete nonalias struct")
	}
	fields, err := xshape.New(holder, lookup).Fields()
	if err != nil {
		return nil, err
	}
	var selectedField *xshape.Field
	for i := range fields {
		if fields[i].Name == fieldName {
			selectedField = &fields[i]
			break
		}
	}
	if selectedField == nil || selectedField.Anonymous || !selectedField.Exported || len(selectedField.Index) != 1 {
		return nil, fmt.Errorf("selector companion holder requires a direct selected component field")
	}
	for _, field := range fields {
		other, present, parseErr := tag.ParseComponent(field.Tag)
		if field.Anonymous || !field.Exported || len(field.Index) != 1 || parseErr != nil || !present || other.ValidateRoute() != nil || !componentField(holder, field) {
			return nil, fmt.Errorf("selector companion holder requires direct component routes")
		}
		if field.ReflectedType != selectedField.ReflectedType || field.TypeExpr != selectedField.TypeExpr {
			return nil, fmt.Errorf("selector companion holder routes require the same contracts")
		}
	}
	component, present, err := tag.ParseComponent(selectedField.Tag)
	if err != nil {
		return nil, err
	}
	if !present || component.ValidateRoute() != nil || !componentField(holder, *selectedField) {
		return nil, fmt.Errorf("selector companion field must declare an exact Component[I,O] route")
	}
	if component.Method != "GET" || component.Handler != "" || component.Settings.Mutation != "" {
		return nil, fmt.Errorf("selector companion requires an ordinary reader")
	}
	identity := selected.Results[0]
	if strings.HasPrefix(identity, "*") {
		return nil, fmt.Errorf("selector companion requires a value result")
	}
	ref, err := (xshape.Resolver{}).Reference(identity)
	if err != nil || len(ref.Arguments) != 0 || ref.Qualifier != holder.PkgPath || !token.IsIdentifier(ref.BaseName) || token.IsExported(ref.BaseName) {
		return nil, fmt.Errorf("selector companion requires a private same-package result")
	}
	var descriptor *x.Type
	if holder.Type != nil {
		base := holder.Type
		if base.Kind() == reflect.Pointer {
			base = base.Elem()
		}
		method, found := reflect.PointerTo(base).MethodByName(Method)
		if !found {
			return nil, fmt.Errorf("selector companion reflected signature missing")
		}
		descriptor = x.NewType(method.Type.Out(0))
	} else if lookup != nil {
		descriptor, err = lookup(identity)
		if err != nil {
			return nil, err
		}
	}
	if descriptor == nil || descriptor.Key() != identity || !concrete(descriptor) {
		return nil, fmt.Errorf("selector companion result must resolve to its exact concrete declaration")
	}
	selectors, err := xshape.New(descriptor, lookup).Fields()
	if err != nil {
		return nil, err
	}
	if len(selectors) == 0 {
		return nil, fmt.Errorf("selector companion has no selectors")
	}
	seen := map[spec.SelectorProperty]map[string]bool{}
	for _, field := range selectors {
		if field.Anonymous || !field.Exported || len(field.Index) != 1 {
			return nil, fmt.Errorf("selector companion fields must be direct and exported")
		}
		metadata, err := tag.ParseField(field.StructField())
		if err != nil {
			return nil, err
		}
		binding := metadata.Binding
		if metadata.QuerySelector == nil || binding == nil || binding.Location.Kind != "query" || strings.TrimSpace(binding.Location.In) == "" {
			return nil, fmt.Errorf("selector companion field %s requires a plain query selector", field.Name)
		}
		if metadata.Codec != nil || len(metadata.Predicates) != 0 || metadata.View != nil || metadata.SQL != nil || len(metadata.Relation) != 0 || metadata.Self != nil || metadata.FormatSelector || binding.Transformer != nil || binding.ResolutionGroup != nil || binding.Scope != "" || binding.When != "" || binding.With != "" || binding.URI != "" || binding.ResourceRef != "" || binding.Async {
			return nil, fmt.Errorf("selector companion field %s has unsupported binding responsibilities", field.Name)
		}
		typ, builtin := field.BuiltinType()
		property := metadata.QuerySelector.Property
		validType := (property == spec.SelectorPropertyFields || property == spec.SelectorPropertyOrderBy) && typ == "[]string" || (property == spec.SelectorPropertyPage || property == spec.SelectorPropertyLimit || property == spec.SelectorPropertyOffset) && typ == "int"
		if !builtin || !validType {
			return nil, fmt.Errorf("selector companion field %s requires Fields/OrderBy []string or Page/Limit/Offset int", field.Name)
		}
		view := strings.TrimSpace(metadata.QuerySelector.View)
		if view == "" {
			return nil, fmt.Errorf("selector companion field %s requires a view", field.Name)
		}
		if seen[property] == nil {
			seen[property] = map[string]bool{}
		}
		if seen[property][view] {
			return nil, fmt.Errorf("selector companion has duplicate selector authority")
		}
		seen[property][view] = true
	}
	return &Contract{Holder: holder.Key(), Field: fieldName, Descriptor: descriptor, Fields: selectors}, nil
}

func concrete(descriptor *x.Type) bool {
	if descriptor == nil || descriptor.Name == "" || descriptor.PkgPath == "" {
		return false
	}
	if typ := descriptor.Type; typ != nil {
		return typ.Kind() == reflect.Struct && typ.Name() != "" && typ.PkgPath() == descriptor.PkgPath && !strings.Contains(typ.Name(), "[")
	}
	if descriptor.SynteticType == nil || descriptor.SynteticType.TypeSpec == nil {
		return false
	}
	decl := descriptor.SynteticType.TypeSpec
	_, ok := decl.Type.(*ast.StructType)
	return ok && !decl.Assign.IsValid() && decl.TypeParams == nil
}

func componentField(holder *x.Type, field xshape.Field) bool {
	const packagePath = "github.com/viant/xdatly"
	if typ := field.ReflectedType; typ != nil {
		return typ.Kind() == reflect.Struct && typ.PkgPath() == packagePath && strings.HasPrefix(typ.Name(), "Component[")
	}
	imports := map[string]string{}
	if holder.SynteticType != nil {
		for alias, imported := range holder.SynteticType.Imports {
			if imported != nil {
				imports[alias] = imported.Path
			}
		}
	}
	resolver := xshape.Resolver{Package: holder.PkgPath, Imports: imports}
	canonical, err := resolver.Canonical(field.TypeExpr)
	if err != nil {
		return false
	}
	ref, err := resolver.Reference(canonical)
	return err == nil && ref.Qualifier == packagePath && ref.BaseName == "Component" && len(ref.Arguments) == 2
}
