package data

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type (
	//Parameter describes parameters used by the Criteria to filter the data.
	Parameter struct {
		shared.Reference
		Name         string
		PresenceName string

		In          *Location
		Required    *bool
		Description string
		Style       string
		Schema      *Schema

		initialized     bool
		view            *View
		xfields         []*xunsafe.Field
		presenceXfields []*xunsafe.Field
	}

	//Location tells how to get parameter value.
	Location struct {
		Kind Kind
		Name string
	}
)

//Init initializes Parameter
func (p *Parameter) Init(ctx context.Context, resource *Resource, structType reflect.Type) error {
	if p.initialized == true {
		return nil
	}

	if p.Ref != "" && p.Name == "" {
		param, err := resource._parameters.Lookup(p.Ref)
		if err != nil {
			return err
		}

		if err = param.Init(ctx, resource, structType); err != nil {
			return err
		}

		p.inherit(param)
	}

	if p.In.Kind == DataViewKind {
		view, err := resource.View(p.In.Name)
		if err != nil {
			return fmt.Errorf("failed to lookup parameter %v view %w", p.Name, err)
		}

		if err = view.Init(ctx, resource); err != nil {
			return err
		}

		p.view = view
	}

	if err := p.initSchema(resource.types, structType); err != nil {
		return err
	}

	err := p.Validate()
	if err != nil {
		return err
	}

	if p.PresenceName == "" {
		p.PresenceName = p.Name
	}

	p.initialized = true
	return nil
}

func (p *Parameter) inherit(param *Parameter) {
	p.Name = notEmptyOf(p.Name, param.Name)
	p.Description = notEmptyOf(p.Description, param.Description)
	p.Style = notEmptyOf(p.Style, param.Style)

	if p.In == nil {
		p.In = param.In
	}

	if p.Required == nil {
		p.Required = param.Required
	}

	if p.Schema == nil {
		p.Schema = param.Schema.copy()
	}
}

//Validate checks if parameter is valid
func (p *Parameter) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("parameter name can't be empty")
	}

	if p.In == nil {
		return fmt.Errorf("parameter location can't be empty")
	}

	if err := p.In.Validate(); err != nil {
		return err
	}

	return nil
}

//View returns View related with Parameter if Location.Kind is set to data_view
func (p *Parameter) View() *View {
	return p.view
}

//Validate checks if Location is valid
func (l *Location) Validate() error {
	if err := l.Kind.Validate(); err != nil {
		return err
	}

	if err := ParamName(l.Name).Validate(l.Kind); err != nil {
		return fmt.Errorf("unsupported param name")
	}

	return nil
}

func (p *Parameter) IsRequired() bool {
	return p.Required != nil && *p.Required == true
}

func (p *Parameter) initSchema(types Types, structType reflect.Type) error {
	if structType != nil {
		return p.initSchemaFromType(structType)
	}

	if p.Schema == nil {
		return fmt.Errorf("parameter %v schema can't be empty", p.Name)
	}

	if p.Schema.DataType == "" {
		return fmt.Errorf("parameter %v schema DataType can't be empty", p.Name)
	}

	return p.Schema.Init(nil, nil, 0, types)
}

func (p *Parameter) initSchemaFromType(structType reflect.Type) error {
	if p.Schema == nil {
		p.Schema = &Schema{}
	}

	segments := strings.Split(p.Name, ".")
	field, err := fieldByTemplateName(structType, segments[0])
	if err != nil {
		return err
	}

	p.Schema.setType(field.Type)
	return p.SetField(structType)
}

func (p *Parameter) UpdatePresence(presencePtr unsafe.Pointer) {
	presencePtr = p.actualStruct(p.presenceXfields, presencePtr)
	p.presenceXfields[len(p.presenceXfields)-1].SetBool(presencePtr, true)
}

func (p *Parameter) SetField(structType reflect.Type) error {
	xFields, err := p.pathFields(p.Name, structType)
	if err != nil {
		return err
	}

	p.xfields = xFields
	return nil
}

func (p *Parameter) pathFields(path string, structType reflect.Type) ([]*xunsafe.Field, error) {
	segments := strings.Split(path, ".")
	if len(segments) == 0 {
		return nil, fmt.Errorf("path can't be empty")
	}

	xFields := make([]*xunsafe.Field, len(segments))

	xField, err := fieldByTemplateName(structType, segments[0])
	if err != nil {
		return nil, err
	}

	xFields[0] = xField
	for i := 1; i < len(segments); i++ {
		newField, err := fieldByTemplateName(xFields[i-1].Type, segments[i])
		if err != nil {
			return nil, err
		}
		xFields[i] = newField
	}
	return xFields, nil
}

func (p *Parameter) Value(values interface{}) (interface{}, error) {
	pointer := xunsafe.AsPointer(values)
	for i := 0; i < len(p.xfields)-1; i++ {
		pointer = p.xfields[i].ValuePointer(pointer)
	}

	xField := p.xfields[len(p.xfields)-1]
	//TODO: Add remaining types
	switch xField.Type.Kind() {
	case reflect.Int:
		return xField.Int(pointer), nil
	case reflect.Float64:
		return xField.Float64(pointer), nil
	case reflect.Bool:
		return xField.Bool(pointer), nil
	case reflect.String:
		return xField.String(pointer), nil
	case reflect.Ptr, reflect.Struct:
		return xField.Value(pointer), nil
	default:
		return nil, fmt.Errorf("unsupported field type %v", xField.Type.String())
	}
}

func (p *Parameter) SetValue(paramPtr unsafe.Pointer, rawValue string) error {
	paramPtr = p.actualStruct(p.xfields, paramPtr)
	//TODO: Add remaining types
	xField := p.xfields[len(p.xfields)-1]
	switch xField.Type.Kind() {
	case reflect.String:
		xField.SetValue(paramPtr, rawValue)
		return nil

	case reflect.Int:
		asInt, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		xField.SetInt(paramPtr, asInt)
		return nil

	case reflect.Bool:
		xField.SetBool(paramPtr, strings.EqualFold(rawValue, "true"))
		return nil

	case reflect.Float64:
		asFloat, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}

		xField.SetFloat64(paramPtr, asFloat)
		return nil
	}

	return fmt.Errorf("unsupported query parameter type %v", xField.Type.String())
}

func (p *Parameter) actualStruct(fields []*xunsafe.Field, paramPtr unsafe.Pointer) unsafe.Pointer {
	prev := paramPtr
	for i := 0; i < len(fields)-1; i++ {
		paramPtr = fields[i].ValuePointer(paramPtr)
		if paramPtr == nil {
			paramPtr = p.initValue(fields[i], prev)
		}

		prev = paramPtr
	}

	if paramPtr == nil && len(fields)-1 >= 0 {
		paramPtr = p.initValue(fields[len(fields)-1], prev)
	}
	return paramPtr
}

func (p *Parameter) initValue(field *xunsafe.Field, prev unsafe.Pointer) unsafe.Pointer {
	value := reflect.New(elem(field.Type))
	if field.Type.Kind() != reflect.Ptr {
		value = value.Elem()
	}

	field.SetValue(prev, value.Interface())
	return unsafe.Pointer(value.Pointer())
}

func elem(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}

	return rType
}

func (p *Parameter) Set(ptr unsafe.Pointer, value interface{}) error {
	ptr = p.actualStruct(p.xfields, ptr)
	p.xfields[len(p.xfields)-1].Set(ptr, value)
	return nil
}

func (p *Parameter) SetPresenceField(structType reflect.Type) error {
	fields, err := p.pathFields(p.PresenceName, structType)
	if err != nil {
		return err
	}

	p.presenceXfields = fields
	return nil
}
