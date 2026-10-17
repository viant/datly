package selectorcompanion

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

// CheckContracts revalidates the association against the selected source or linked contracts.
func CheckContracts(component *spec.Component, input, output *x.Type, lookup xshape.Lookup) (*Contract, error) {
	if component == nil || component.SelectorCompanion == nil || input == nil || output == nil {
		return nil, fmt.Errorf("selector companion requires exact canonical contracts")
	}
	association := component.SelectorCompanion
	holder, err := association.Holder()
	if err != nil {
		return nil, err
	}
	result, err := association.Companion()
	if err != nil {
		return nil, err
	}
	fallback := lookup
	lookup = func(identity string) (*x.Type, error) {
		if identity == result.Key() {
			return result, nil
		}
		if fallback != nil {
			return fallback(identity)
		}
		return nil, nil
	}
	checked, err := Resolve(holder, association.Field(), lookup)
	if err != nil || checked == nil {
		if err == nil {
			err = fmt.Errorf("selector declaration missing")
		}
		return nil, err
	}
	if component.Key.Kind != spec.KindComponent || holder.PkgPath != component.Key.Scope || checked.Descriptor.Key() != result.Key() {
		return nil, fmt.Errorf("selector companion does not belong to the exact component package")
	}
	fields, err := xshape.New(holder, lookup).Fields()
	if err != nil {
		return nil, err
	}
	field := fields[0]
	metadata, present, err := tag.ParseComponent(field.Tag)
	if err != nil || !present {
		return nil, fmt.Errorf("selector holder route declaration missing")
	}
	name := metadata.Name
	if name == "" {
		name = association.Field()
	}
	if component.Key.Name != name || component.Name != name || len(component.Routes) != 1 || component.Routes[0] == nil || component.Routes[0].Method != metadata.Method || component.Routes[0].Path != metadata.Path {
		return nil, fmt.Errorf("selector companion route identity mismatch")
	}
	// A genuine reader declaration cannot grant selector storage to execution
	// metadata that has since selected a handler or mutation owner.
	if component.Routes[0].Handler != "" {
		return nil, fmt.Errorf("selector companion requires the current ordinary reader execution policy")
	}
	if settings := component.Settings; settings != nil && (settings.Mutation != "" || settings.SequenceStrategy != "" || settings.ComponentCallPolicy != "" || settings.IndependentChildTransactions || len(settings.ProtectedFlushTables) != 0 || settings.WarmupTarget != nil) {
		return nil, fmt.Errorf("selector companion requires the current ordinary reader execution policy")
	}
	publicFields, err := xshape.New(input, lookup).Fields()
	if err != nil {
		return nil, err
	}
	for _, selector := range checked.Fields {
		for _, public := range publicFields {
			if selector.Name == public.Name {
				return nil, fmt.Errorf("selector companion duplicates public input field %s", selector.Name)
			}
		}
	}
	var arguments []string
	if field.ReflectedType != nil {
		in, _ := field.ReflectedType.FieldByName("Inout")
		if in.Type == nil {
			in, _ = field.ReflectedType.FieldByName("Input")
		}
		out, _ := field.ReflectedType.FieldByName("Output")
		for _, typ := range []xshape.Field{{ReflectedType: in.Type}, {ReflectedType: out.Type}} {
			expression, err := (xshape.Resolver{Qualifier: func(location, suggested string) string { return location }}).Expression(typ.ReflectedType)
			if err != nil {
				return nil, err
			}
			arguments = append(arguments, expression)
		}
	} else {
		imports := map[string]string{}
		for alias, imported := range holder.SynteticType.Imports {
			if imported != nil {
				imports[alias] = imported.Path
			}
		}
		resolver := xshape.Resolver{Package: holder.PkgPath, Imports: imports}
		expression, err := resolver.Canonical(field.TypeExpr)
		if err != nil {
			return nil, err
		}
		ref, err := resolver.Reference(expression)
		if err != nil {
			return nil, err
		}
		arguments = ref.Arguments
	}
	for i, actual := range []*x.Type{input, output} {
		if actual == nil || len(arguments) != 2 {
			return nil, fmt.Errorf("selector companion requires exact canonical contracts")
		}
		if actual.Type != nil {
			if err := (xshape.Contract{}).ValidateLinked(arguments[i], actual.Type); err != nil {
				return nil, fmt.Errorf("selector holder contract mismatch: %w", err)
			}
		} else if strings.TrimSpace(arguments[i]) != actual.Key() {
			return nil, fmt.Errorf("selector holder source contract mismatch")
		}
	}
	return checked, nil
}
