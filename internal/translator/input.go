package translator

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
	"reflect"
	"strings"
)

func (s *Service) updateExplicitInputType(resource *Resource, viewlet *Viewlet) error {
	if resource.rule.Generated {
		return nil
	}
	registry := resource.typeRegistry
	inputState := inference.State{}
	for i, item := range resource.State {
		s.updatedNamedType(&item.Parameter, registry)
		if strings.Contains(item.Name, ".") {
			continue
		}
		if err := s.adjustCodecOutputType(&item.Parameter, resource.typeRegistry, resource); err != nil {
			return err
		}
		inputState.Append(resource.State[i])
	}

	inputState.Append(resource.AsyncState...)
	inputParameters := inputState.Parameters()

	resource.Rule.Input.Type.Parameters = inputParameters
	if resource.Rule.Input.Type.Schema == nil {
		resource.Rule.Input.Type.Schema = &state.Schema{}
	}
	resource.Rule.Input.Type.Package = resource.rule.Package()
	resource.Rule.Input.Type.Name = state.SanitizeTypeName(viewlet.Name) + "Input"

	res := view.NewResources(&resource.Resource, &viewlet.View.View)
	res.Parameters = inputParameters

	s.tryToBuildNamedInputType(resource, resource.Rule.Input.Type, res)
	return nil
}

func (s *Service) updatedNamedType(item *state.Parameter, registry *xreflect.Types) {
	if schema := item.Schema; schema != nil {
		if schema.Name != "" {
			if rType, _ := registry.Lookup(schema.Name, xreflect.WithPackage(schema.Package)); rType != nil && rType.Name() != "" {
				schema.SetType(rType)
				prefix := ""
				if rType.Kind() == reflect.Struct {
					prefix = "*"
				}
				schema.DataType = prefix + rType.Name()
			}
		}
	}
}

func (s *Service) tryToBuildNamedInputType(resource *Resource, aType state.Type, res *view.Resourcelet) {
	//if you failed at translation i.e due to missing custom codec or other dependencies, it would try to build later at runtime
	err := aType.Init(state.WithResource(res))
	if err != nil || aType.Schema == nil {
		aType.Name = ""
		return
	}
	rType := aType.Schema.Type()
	if aType.Name == "" || rType == nil {
		return
	}

	//TO change to use xreflect.Type.Body for rType
	sType := types.EnsureStruct(rType)
	var typeDefs []*view.TypeDefinition
	for _, parameter := range aType.Parameters {
		anObject, ok := sType.FieldByName(parameter.Name)
		if !ok {
			return
		}
		fieldTypeName := anObject.Tag.Get(xreflect.TagTypeName)
		if fieldTypeName == "" {
			continue
		}
		fieldType := types.EnsureStruct(anObject.Type)
		if fieldType != nil {
			if hasField, ok := fieldType.FieldByName("Has"); ok {
				if hasFieldName := hasField.Tag.Get(xreflect.TagTypeName); hasFieldName != "" {
					typeDefs = append(typeDefs, buildTypeDef(hasFieldName, aType.Package, hasField.Type))
				}
			}
		}
		typeDefs = append(typeDefs, buildTypeDef(fieldTypeName, aType.Package, anObject.Type))
	}

	for _, aDef := range typeDefs {
		resource.AppendTypeDefinition(aDef)
	}
	aTypeDef := buildTypeDef(aType.Name, aType.Package, rType)
	resource.AppendTypeDefinition(aTypeDef)

}

func buildTypeDef(name string, pkg string, rType reflect.Type) *view.TypeDefinition {
	xType := xreflect.NewType(name, xreflect.WithPackage(pkg), xreflect.WithReflectType(rType))
	aTypeDef := &view.TypeDefinition{Name: name, Package: pkg, DataType: xType.Body()}
	return aTypeDef
}
