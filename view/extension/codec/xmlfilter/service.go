package xmlfilter

import (
	"fmt"
	"github.com/viant/tagly/format"
	"github.com/viant/xdatly/handler/response/tabular/xml"
	"github.com/viant/xmlify"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type Service struct{}

func (t *Service) Transfer(aStruct interface{}) (*xml.FilterHolder, error) {

	structType := reflect.TypeOf(aStruct)
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("xmlfilter: expected struct but had: %T", aStruct)
	}

	ptr := xunsafe.AsPointer(aStruct)
	xFilters := xunsafe.NewStruct(structType)

	result := &xml.FilterHolder{}

	for _, field := range xFilters.Fields {
		aTag, _ := format.Parse(field.Tag, xmlify.TagName)
		if aTag != nil && aTag.Ignore {
			continue
		}

		//fmt.Printf("### %s %s %s\n", field.Name, field.Kind(), field.Type.String())
		fieldType := field.Type

		var xFilterPtr unsafe.Pointer
		if fieldType.Kind() == reflect.Ptr {
			ownerAddr := field.Pointer(ptr)
			xFilterPtr = *(*unsafe.Pointer)(ownerAddr)
			fieldType = fieldType.Elem()
		} else {
			xFilterPtr = unsafe.Pointer(uintptr(ptr) + field.Offset)
		}

		if fieldType.Kind() != reflect.Struct {
			return nil, fmt.Errorf("xmlfilter: expected struct but had: %T", aStruct)
		}

		if xFilterPtr == nil {
			continue
		}

		xFilter := xunsafe.NewStruct(fieldType)

		filter, err := t.transferFilterObject(xFilter, xFilterPtr, field.Name)
		if err != nil {
			return nil, err
		}
		if aTag != nil {
			filter.Tag = *aTag
		}

		result.Filters = append(result.Filters, filter)
	}

	//toolbox.DumpIndent(result, false)

	return result, nil

}

func (t *Service) transferFilterObject(xFieldStruct *xunsafe.Struct, ptr unsafe.Pointer, name string) (*xml.Filter, error) {
	const include = "Include"
	const exclude = "Exclude"
	filterObj := &xml.Filter{Name: name}

	for _, subField := range xFieldStruct.Fields {
		//fmt.Printf("###### name: %s kind: %s type: %s\n", subField.Name, subField.Kind(), subField.Type.String())
		subFieldType := subField.Type
		if subField.Kind() == reflect.Ptr {
			subFieldType = subFieldType.Elem()
		}
		if subField.Kind() != reflect.Slice {
			return nil, fmt.Errorf("xmlfilter: expected slice but had: %T", subField)
		}

		slicePtr := unsafe.Add(ptr, subField.Offset)
		switch elemTypeName := subFieldType.Elem().Name(); elemTypeName {
		case "int":
			s := (*[]int)(slicePtr)
			//fmt.Println(*s)
			if subField.Name == include {
				filterObj.IncludeInt = *s
			}
			if subField.Name == exclude {
				filterObj.ExcludeInt = *s
			}
		case "string":
			s := (*[]string)(slicePtr)
			//fmt.Println(*s)
			if subField.Name == include {
				filterObj.IncludeString = *s
			}
			if subField.Name == exclude {
				filterObj.ExcludeString = *s
			}
		case "bool":
			s := (*[]bool)(slicePtr)
			//fmt.Println(*s)
			if subField.Name == include {
				filterObj.IncludeBool = *s
			}
			if subField.Name == exclude {
				filterObj.ExcludeBool = *s
			}
		default:
			//TODO
			fmt.Printf("xmlfilter: unsupported type %s\n", elemTypeName)
		}
	}
	return filterObj, nil
}

func New() *Service {
	return &Service{}
}
