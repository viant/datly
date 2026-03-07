package scan

import (
	"fmt"
	"path"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/componenttag"
	"github.com/viant/datly/repository/shape/typectx"
)

const (
	xdatlyComponentPkg  = "github.com/viant/xdatly"
	xdatlyComponentName = "Component["
)

type componentContract struct {
	InputType   reflect.Type
	OutputType  reflect.Type
	InputName   string
	OutputName  string
	UsesDynamic bool
}

func resolveComponentContract(source *shape.Source, fieldType reflect.Type, value reflect.Value, tag *componenttag.Tag) (*componentContract, error) {
	contract := &componentContract{}
	if tag != nil && tag.Component != nil {
		contract.InputName = strings.TrimSpace(tag.Component.Input)
		contract.OutputName = strings.TrimSpace(tag.Component.Output)
	}
	if provider := componentContractProvider(value); provider != nil {
		contract.InputType = provider.ComponentInputType()
		contract.OutputType = provider.ComponentOutputType()
	}
	typedInput, typedOutput, dynamic := inspectXDatlyComponent(fieldType, value)
	isStructuredHolder := providerDefined(contract) || isXDatlyComponentType(fieldType)
	if contract.InputType == nil {
		contract.InputType = typedInput
	}
	if contract.OutputType == nil {
		contract.OutputType = typedOutput
	}
	if contract.InputType == nil && contract.InputName != "" {
		contract.InputType = resolveNamedComponentContractType(source, contract.InputName)
	}
	if contract.OutputType == nil && contract.OutputName != "" {
		contract.OutputType = resolveNamedComponentContractType(source, contract.OutputName)
	}
	contract.UsesDynamic = dynamic
	if !isStructuredHolder && contract.InputName == "" && contract.OutputName == "" {
		return contract, nil
	}
	if contract.UsesDynamic &&
		contract.InputType == nil &&
		contract.OutputType == nil &&
		contract.InputName == "" &&
		contract.OutputName == "" {
		return nil, fmt.Errorf("dynamic component holder requires explicit input/output tag names or initialized Inout/Output values")
	}
	if contract.InputType == nil && contract.InputName == "" {
		return nil, fmt.Errorf("component input contract type is unresolved")
	}
	if contract.OutputType == nil && contract.OutputName == "" {
		return nil, fmt.Errorf("component output contract type is unresolved")
	}
	return contract, nil
}

func resolveNamedComponentContractType(source *shape.Source, typeName string) reflect.Type {
	typeName = strings.TrimSpace(typeName)
	if source == nil || typeName == "" {
		return nil
	}
	registry := source.EnsureTypeRegistry()
	if registry == nil {
		return nil
	}
	resolver := typectx.NewResolver(registry, componentTypeContext(source))
	resolved, err := resolver.Resolve(typeName)
	if err != nil || resolved == "" {
		return nil
	}
	lookup := registry.Lookup(resolved)
	if lookup == nil || lookup.Type == nil {
		return nil
	}
	return unwrapComponentType(lookup.Type)
}

func componentTypeContext(source *shape.Source) *typectx.Context {
	if source == nil {
		return nil
	}
	rootType, err := source.ResolveRootType()
	if err != nil || rootType == nil {
		return nil
	}
	pkgPath := strings.TrimSpace(rootType.PkgPath())
	if pkgPath == "" {
		return nil
	}
	return &typectx.Context{
		DefaultPackage: pkgPath,
		PackagePath:    pkgPath,
		PackageName:    path.Base(pkgPath),
	}
}

func providerDefined(contract *componentContract) bool {
	return contract != nil && (contract.InputType != nil || contract.OutputType != nil)
}

type typedComponentContract interface {
	ComponentInputType() reflect.Type
	ComponentOutputType() reflect.Type
}

func componentContractProvider(value reflect.Value) typedComponentContract {
	if !value.IsValid() {
		return nil
	}
	if value.CanInterface() {
		if provider, ok := value.Interface().(typedComponentContract); ok {
			return provider
		}
	}
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if value.CanInterface() {
		if provider, ok := value.Interface().(typedComponentContract); ok {
			return provider
		}
	}
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
		if !value.CanInterface() {
			continue
		}
		if provider, ok := value.Interface().(typedComponentContract); ok {
			return provider
		}
	}
	return nil
}

func componentFieldValue(holder reflect.Value, fieldName string) reflect.Value {
	if !holder.IsValid() {
		return reflect.Value{}
	}
	for holder.IsValid() && holder.Kind() == reflect.Ptr {
		if holder.IsNil() {
			return reflect.Value{}
		}
		holder = holder.Elem()
	}
	if !holder.IsValid() || holder.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	field := holder.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}
	}
	return field
}

func concreteComponentFieldType(fallback reflect.Type, holder reflect.Value, fieldName string) (reflect.Type, bool) {
	if fallback != nil && !(fallback.Kind() == reflect.Interface && fallback.NumMethod() == 0) {
		return fallback, false
	}
	field := componentFieldValue(holder, fieldName)
	if !field.IsValid() {
		return nil, true
	}
	for field.IsValid() && field.Kind() == reflect.Interface {
		if field.IsNil() {
			return nil, true
		}
		field = field.Elem()
	}
	for field.IsValid() && field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return nil, true
		}
		field = field.Elem()
	}
	if !field.IsValid() {
		return nil, true
	}
	return field.Type(), true
}

func inspectXDatlyComponent(rType reflect.Type, value reflect.Value) (reflect.Type, reflect.Type, bool) {
	rType = unwrapComponentType(rType)
	if !isXDatlyComponentType(rType) {
		return nil, nil, false
	}
	inoutField, ok := rType.FieldByName("Inout")
	if !ok {
		return nil, nil, false
	}
	outputField, ok := rType.FieldByName("Output")
	if !ok {
		return nil, nil, false
	}
	inputType, inputDynamic := concreteComponentFieldType(inoutField.Type, value, "Inout")
	outputType, outputDynamic := concreteComponentFieldType(outputField.Type, value, "Output")
	return inputType, outputType, inputDynamic || outputDynamic
}

func isXDatlyComponentType(rType reflect.Type) bool {
	rType = unwrapComponentType(rType)
	return rType != nil && rType.Kind() == reflect.Struct && rType.PkgPath() == xdatlyComponentPkg && strings.HasPrefix(rType.Name(), xdatlyComponentName)
}

func unwrapComponentType(rType reflect.Type) reflect.Type {
	for rType != nil && rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}
