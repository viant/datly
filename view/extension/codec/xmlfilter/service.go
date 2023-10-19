package xmlfilter

import (
	"fmt"
	"github.com/viant/structology/format"
	"github.com/viant/xmlify"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type (
	Filter struct {
		Name          string
		Tag           format.Tag
		IncludeString []string `json:",omitempty" xmlify:"omitempty"`
		ExcludeString []string `json:",omitempty" xmlify:"omitempty"`
		IncludeInt    []int    `json:",omitempty" xmlify:"omitempty"`
		ExcludeInt    []int    `json:",omitempty" xmlify:"omitempty"`
		IncludeBool   []bool   `json:",omitempty" xmlify:"omitempty"`
		ExcludeBool   []bool   `json:",omitempty" xmlify:"omitempty"`
	}

	Result struct {
		Filters []*Filter
	}
)

func (f *Result) MarshalXML() ([]byte, error) {

	var sb strings.Builder

	sb.WriteString("<filter>")

	for _, filter := range f.Filters {
		wasInclusion := false

		//TODO check if filter is empty
		sb.WriteString("\n")
		sb.WriteString("<")

		if filter.Tag.Name != "" {
			sb.WriteString(filter.Tag.Name)
		} else {
			sb.WriteString(filter.Name)
		}

		sb.WriteString(" ")

		switch {
		case filter.IncludeInt != nil:
			sb.WriteString(`include-ids="`)
			for i, value := range filter.IncludeInt {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(strconv.Itoa(value))
			}
			sb.WriteString(`"`)
			wasInclusion = true
		case filter.IncludeString != nil:
			sb.WriteString(`include-ids="`)
			for i, value := range filter.IncludeString {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(value)
			}
			sb.WriteString(`"`)
			wasInclusion = true
		case filter.IncludeBool != nil:
			sb.WriteString(`include-ids="`)
			for i, value := range filter.IncludeBool {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(strconv.FormatBool(value))
			}
			sb.WriteString(`"`)
			wasInclusion = true
		}

		isExclusion := filter.ExcludeInt != nil || filter.ExcludeString != nil || filter.ExcludeBool != nil
		if wasInclusion && isExclusion {
			sb.WriteString(` `)
		}

		switch {
		case filter.ExcludeInt != nil:
			sb.WriteString(`exclude-ids="`)
			for i, value := range filter.ExcludeInt {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(strconv.Itoa(value))
			}
			sb.WriteString(`"`)
		case filter.ExcludeString != nil:
			sb.WriteString(`exclude-ids="`)
			for i, value := range filter.ExcludeString {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(value)
			}
			sb.WriteString(`"`)
		case filter.ExcludeBool != nil:
			sb.WriteString(`exclude-ids="`)
			for i, value := range filter.ExcludeBool {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(strconv.FormatBool(value))
			}
			sb.WriteString(`"`)
		}

		sb.WriteString("/>")
	}

	sb.WriteString("\n")
	sb.WriteString("</filter>")
	//fmt.Println(sb.String())
	return []byte(sb.String()), nil
}

type Service struct{}

func (t *Service) Transfer(aStruct interface{}) (*Result, error) {

	structType := reflect.TypeOf(aStruct)
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	if structType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("xmlfilter: expected struct but had: %T", aStruct)
	}

	ptr := xunsafe.AsPointer(aStruct)
	xFilters := xunsafe.NewStruct(structType)

	result := &Result{}

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

func (t *Service) transferFilterObject(xFieldStruct *xunsafe.Struct, ptr unsafe.Pointer, name string) (*Filter, error) {
	const include = "Include"
	const exclude = "Exclude"
	filterObj := &Filter{Name: name}

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
