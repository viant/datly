package generate

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type fieldTypeResolver struct {
	plan          *Plan
	context       *spec.TypeContext
	targetPackage string
	lookup        func(string) (*x.Type, error)
}

func (p *planResolver) concretizeFields(fields []Field, output bool) error {
	if len(fields) == 0 || p.input.Component == nil {
		return nil
	}
	params := map[string]*spec.Parameter{}
	for _, param := range preferDefinedParams(p.input.Component.Parameters) {
		if param == nil {
			continue
		}
		// Input and output may deliberately share an exported name (for example
		// Data: request IDs vs response rows). Resolve within the same contract
		// direction as resolveInputFields/resolveOutputFields; a name-only map
		// across both contracts silently replaces the type by declaration order.
		isOutput := param.EmitOutput || strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output")
		if isOutput != output {
			continue
		}
		params[generatedParameterName(param)] = param
	}
	resolver := fieldTypeResolver{
		plan: p.plan, context: p.input.Component.TypeContext, targetPackage: p.input.TargetPackage, lookup: p.lookup(),
	}
	for index := range fields {
		param := params[fields[index].Name]
		if param == nil {
			continue
		}
		authored := strings.TrimSpace(param.OutputTypeExpr)
		if authored == "" {
			authored = strings.TrimSpace(param.TypeExpr)
		}
		if authored == "" {
			authored = defaultTagTypeName(param.Tag)
		}
		if authored == "" {
			continue
		}
		resolved, err := resolver.resolve(authored)
		if err != nil {
			return err
		}
		fields[index].Type = resolved
	}
	return nil
}

func (r *fieldTypeResolver) resolve(authored string) (string, error) {
	authored = strings.TrimSpace(authored)
	if strings.Contains(unwrapQualifiedTypeName(authored), "/") {
		return r.resolveFullPackagePath(authored, nil)
	}
	resolved, err := (xshape.Resolver{Rewriter: r.rewriteNamed}).Rewrite(authored)
	if err != nil {
		return "", fmt.Errorf("resolve plan field type %q: %w", authored, err)
	}
	return resolved, nil
}

func (r *fieldTypeResolver) resolveFullPackagePath(authored string, parseErr error) (string, error) {
	base := unwrapQualifiedTypeName(authored)
	if !strings.Contains(base, "/") || !strings.Contains(base, ".") {
		return "", fmt.Errorf("resolve plan field type %q: invalid Go type expression: %v", authored, parseErr)
	}
	if local, err := r.qualifiedGeneratedType(base); err != nil {
		return "", err
	} else if local != "" {
		return applyQualifiedWrapper(authored, local), nil
	}
	descriptor, err := r.requiredDescriptor(authored)
	if err != nil {
		return "", err
	}
	return applyQualifiedWrapper(authored, r.emittedNamedType(base, descriptor)), nil
}

func (r *fieldTypeResolver) hasLocalType(name string) bool {
	name = strings.TrimSpace(name)
	if r.plan == nil || name == "" {
		return false
	}
	// Generated contracts are local declaration authority even before their Go
	// files exist. Linked or separately emitted contracts still require lookup.
	for _, contract := range []ContractPlan{r.plan.Input, r.plan.Output} {
		if contract.Ownership == ContractGenerated && strings.TrimSpace(contract.Type) == name &&
			(contract.Package == "" || contract.Package == r.targetPackage) {
			return true
		}
	}
	for _, view := range r.plan.Views {
		if view.Ownership == ViewGenerated && (view.Package == "" || view.Package == r.targetPackage) && strings.TrimSpace(view.Type) == name && isTypeIdentifier(view.Type) {
			return true
		}
	}
	for _, helper := range r.plan.HelperTypes {
		if (helper.Package == "" || helper.Package == r.targetPackage) && strings.TrimSpace(helper.Name) == name {
			return true
		}
	}
	return false
}

// Qualified references to declarations this plan emits do not require an
// existing Go descriptor. Keep that proof distinct from discovered Go types.
func (r *fieldTypeResolver) qualifiedGeneratedType(authored string) (string, error) {
	if r.plan == nil || r.targetPackage == "" || !strings.Contains(authored, ".") {
		return "", nil
	}
	canonical, err := r.plan.CanonicalType(r.targetPackage, authored)
	if err != nil {
		return "", err
	}
	prefix := r.targetPackage + "."
	if !strings.HasPrefix(canonical, prefix) {
		return "", nil
	}
	name := strings.TrimPrefix(canonical, prefix)
	if !isTypeIdentifier(name) || !r.hasLocalType(name) {
		return "", nil
	}
	return name, nil
}

func (r *fieldTypeResolver) rewriteNamed(authored string) (string, error) {
	if !strings.Contains(authored, ".") && (r.hasLocalType(authored) || !r.hasExternalDefaultPackage()) {
		return authored, nil
	}
	if local, err := r.qualifiedGeneratedType(authored); err != nil {
		return "", err
	} else if local != "" {
		return local, nil
	}
	descriptor, err := r.requiredDescriptor(authored)
	if err != nil {
		return "", fmt.Errorf("resolve plan field type %q: %w", authored, err)
	}
	return r.emittedNamedType(authored, descriptor), nil
}

func (r *fieldTypeResolver) requiredDescriptor(authored string) (*x.Type, error) {
	if r.lookup == nil {
		if descriptor := r.standardDescriptor(authored); descriptor != nil {
			return descriptor, nil
		}
		return nil, fmt.Errorf("package type requires type authority")
	}
	descriptor, err := r.lookup(authored)
	if err != nil {
		return nil, err
	}
	if descriptor == nil {
		return nil, fmt.Errorf("package type was not found")
	}
	return descriptor, nil
}

func (r *fieldTypeResolver) standardDescriptor(authored string) *x.Type {
	if strings.TrimSpace(authored) == "encoding/json.RawMessage" {
		return x.NewType(reflect.TypeFor[json.RawMessage]())
	}
	if strings.TrimSpace(authored) == "time.Time" {
		return x.NewType(reflect.TypeFor[time.Time]())
	}
	qualifier, name, found := strings.Cut(strings.TrimSpace(authored), ".")
	if !found || r.context == nil {
		return nil
	}
	packagePath := ""
	for _, item := range r.context.Imports {
		if item.Alias == qualifier {
			packagePath = item.Package
			break
		}
	}
	var typeOf reflect.Type
	switch packagePath + "." + name {
	case "github.com/viant/xdatly/response.Status":
		typeOf = reflect.TypeFor[xresponse.Status]()
	case "github.com/viant/xdatly/response.Metrics":
		typeOf = reflect.TypeFor[xresponse.Metrics]()
	case "github.com/viant/xdatly/handler.Violation":
		typeOf = reflect.TypeFor[xhandler.Violation]()
	case "encoding/json.RawMessage":
		typeOf = reflect.TypeFor[json.RawMessage]()
	}
	if typeOf == nil {
		return nil
	}
	return x.NewType(typeOf)
}

func (r *fieldTypeResolver) emittedNamedType(authored string, descriptor *x.Type) string {
	authored = strings.TrimSpace(authored)
	if descriptor == nil || strings.TrimSpace(descriptor.Name) == "" {
		return ""
	}
	packagePath := strings.TrimSpace(descriptor.PkgPath)
	if packagePath != "" && r.plan != nil {
		if r.plan.resolvedNamedTypes == nil {
			r.plan.resolvedNamedTypes = map[string]bool{}
		}
		r.plan.resolvedNamedTypes[packagePath+"."+descriptor.Name] = true
	}
	if packagePath != "" && packagePath == strings.TrimSpace(r.targetPackage) {
		if r.plan != nil {
			if r.plan.knownLocalTypes == nil {
				r.plan.knownLocalTypes = map[string]string{}
			}
			r.plan.knownLocalTypes[descriptor.Name] = packagePath + "." + descriptor.Name
		}
		return descriptor.Name
	}
	if qualifier, name, found := strings.Cut(unwrapQualifiedTypeName(authored), "."); found && name == descriptor.Name && !strings.Contains(qualifier, "/") && r.context != nil {
		for _, item := range r.context.Imports {
			if item.Alias == qualifier {
				ensureImport(r.plan, qualifier, item.Package)
				return authored
			}
		}
	}
	if packagePath == "" {
		return descriptor.Name
	}
	if qualifier, name, found := strings.Cut(unwrapQualifiedTypeName(authored), "."); found && name == descriptor.Name && !strings.Contains(qualifier, "/") {
		if qualifier == packageAlias(packagePath) {
			ensureImport(r.plan, qualifier, packagePath)
			return authored
		}
	}
	alias := uniqueImportAlias(r.plan, packagePath)
	ensureImport(r.plan, alias, packagePath)
	if strings.Contains(authored, ".") && !strings.Contains(authored, "/") && strings.HasPrefix(unwrapQualifiedTypeName(authored), alias+".") {
		return authored
	}
	return alias + "." + descriptor.Name
}

func (r *fieldTypeResolver) hasExternalDefaultPackage() bool {
	if r.context == nil {
		return false
	}
	defaultPackage := strings.TrimSpace(r.context.DefaultPackage)
	return defaultPackage != "" && defaultPackage != strings.TrimSpace(r.targetPackage)
}
