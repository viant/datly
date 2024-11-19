package translator

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

func (s *Service) updateExplicitInputType(resource *Resource, viewlet *Viewlet) error {
	if resource.rule.Generated {
		return nil
	}
	registry := resource.typeRegistry
	inputState := inference.State{}
	rootViewlet := resource.Rule.RootViewlet()

	for i, item := range resource.State {
		s.updatedNamedType(&item.Parameter, registry)
		if strings.Contains(item.Name, ".") {
			continue
		}
		if err := s.adjustCodecType(&item.Parameter, resource.typeRegistry, resource); err != nil {
			return err
		}
		inputState.Append(resource.State[i])
		if item.In.Kind == state.KindRequestBody {
			if rootViewlet.View != nil && rootViewlet.View.Schema != nil {
				if rType := rootViewlet.View.Schema.Type(); rType != nil {
					item.Schema.SetType(rType)
				}
			}
		}
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
	err := aType.Init(state.WithResource(res), state.WithMarker(true))
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
	var markerField *reflect.StructField
	for _, parameter := range aType.Parameters {
		if parameter.Schema.DataType != "" {
			continue
		}
		aStructField, ok := sType.FieldByName(parameter.Name)
		if !ok {
			return
		}
		if markerTag := aStructField.Tag.Get(structology.SetMarkerTag); markerTag != "" {
			markerField = &aStructField
		}
		fieldTypeName := aStructField.Tag.Get(xreflect.TagTypeName)
		if fieldTypeName == "" && types.EnsureStruct(aStructField.Type) != nil {
			if typeName := aStructField.Type.Name(); typeName != "" && !strings.Contains(typeName, ".") {
				fieldTypeName = typeName

			}
		}
		if fieldTypeName == "" {
			continue
		}
		fieldType := types.EnsureStruct(aStructField.Type)
		if fieldType != nil {
			if hasTypeDef := buildMarkerTypeDef(fieldType, aType, fieldTypeName); hasTypeDef != nil {
				typeDefs = append(typeDefs, hasTypeDef)
			}
		}
		parameter.Schema.DataType = fieldTypeName
		pkg := parameter.Schema.Package
		if pkg == "" {
			parameter.Schema.Package = aType.Package
		}
		typeDefs = append(typeDefs, buildTypeDef(fieldTypeName, parameter.Schema.Package, aStructField.Type))
	}

	if markerField == nil {
		xStruct := xunsafe.NewStruct(rType)
		for i := range xStruct.Fields {
			field := &xStruct.Fields[i]
			if markerTag := field.Tag.Get(structology.SetMarkerTag); markerTag != "" {
				markerField = &reflect.StructField{Name: field.Name, Type: field.Type, Tag: reflect.StructTag(field.Tag)}
			}
		}
	}
	if markerField != nil {
		typeName := markerField.Name
		if fieldTypeName := markerField.Tag.Get(xreflect.TagTypeName); fieldTypeName != "" {
			typeName = fieldTypeName
		}
		markerType := markerField.Type
		if markerType.Kind() == reflect.Ptr {
			markerType = markerType.Elem()
		}
		if markerTypeName := markerType.Name(); markerTypeName != "" {
			typeName = markerTypeName
		}
		typeDefs = append(typeDefs, buildTypeDef(typeName, aType.Package, types.InlineStruct(markerField.Type, nil)))
	}

	for _, aDef := range typeDefs {
		resource.AppendTypeDefinition(aDef)
	}
	aTypeDef := buildTypeDef(aType.Name, aType.Package, rType)
	resource.AppendTypeDefinition(aTypeDef)
}

func buildMarkerTypeDef(fieldType reflect.Type, aType state.Type, holderName string) *view.TypeDefinition {
	for i := 0; i < fieldType.NumField(); i++ {
		field := fieldType.Field(i)
		if field.Tag.Get(structology.SetMarkerTag) != "" {
			typeName := field.Tag.Get(xreflect.TagTypeName)
			if typeName == "" {
				typeName = field.Type.Name()
			}
			if typeName == "" {
				typeName = holderName + field.Name
			}
			if typeName != "" {
				return buildTypeDef(typeName, aType.Package, field.Type)
				break
			}
		}
	}
	return nil
}

func buildTypeDef(name string, pkg string, rType reflect.Type) *view.TypeDefinition {
	xType := xreflect.NewType(name, xreflect.WithPackage(pkg), xreflect.WithReflectType(rType))
	body := xType.Body()
	aTypeDef := &view.TypeDefinition{Name: name, Package: pkg, DataType: body}
	return aTypeDef
}
