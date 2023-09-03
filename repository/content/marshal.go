package content

import (
	"fmt"
	"github.com/viant/datly/gateway/router/marshal/json"
	"reflect"
	"strings"
)

// Marshal marshals response
// TODO refactor it to take just a marshaller for format, marshaller should only deal with response
// (requiring field, reader data seems code design issue)
func (c *Content) Marshal(format string, field string, response interface{}, options ...interface{}) ([]byte, error) {
	switch strings.ToLower(format) {
	case XLSFormat:
		return c.Marshaller.XLS.XlsMarshaller.Marshal(response)
	case CSVFormat:
		response = ensureSliceValue(response)
		return c.CSV.OutputMarshaller.Marshal(response)
	case XMLFormat: //TODO MFI refactor
		if c.XML.OutputMarshaller == nil {
			rType := reflect.TypeOf(response)
			exclude := []string{}
			var inputType reflect.Type
			if err := c.initXMLIfNeeded(exclude, inputType, rType); err != nil {
				return nil, err
			}
		}
		return c.XML.OutputMarshaller.Marshal(response)
	case JSONDataFormatTabular:
		if field != "" {
			responseData := ensureSliceValue(response)
			tabJSONInterceptors := c.tabJSONInterceptors(field, responseData)
			return c.JsonMarshaller.Marshal(response, tabJSONInterceptors)
		}
		return c.TabularJSON.OutputMarshaller.Marshal(response, options...)
	case JSONFormat:
		return c.JsonMarshaller.Marshal(response, options...)
	default:
		return nil, fmt.Errorf("unsupproted readerData format: %s", format)
	}
	//TODO extract responseData
}

func ensureSliceValue(v interface{}) interface{} {
	rType := reflect.TypeOf(v)
	if rType.Kind() == reflect.Slice {
		return v
	}
	rValue := reflect.ValueOf(v)
	destType := rType
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
		rValue = rValue.Elem()
	}

	switch destType.Kind() {
	case reflect.Struct:
		for i := 0; i < destType.NumField(); i++ {
			wasPtr := false
			field := destType.Field(i)
			fieldType := field.Type
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
				wasPtr = true
			}
			if fieldType.Kind() == reflect.Slice {
				candidate := fieldType.Elem()
				if candidate.Kind() == reflect.Struct || (candidate.Kind() == reflect.Ptr && candidate.Elem().Kind() == reflect.Struct) {
					result := rValue.Field(i)
					if wasPtr {
						result = result.Elem()
					}
					return result.Interface()
				}
			}
		}
	}
	return rType
}

func (c *Content) tabJSONInterceptors(field string, data interface{}) json.MarshalerInterceptors {
	interceptors := make(map[string]json.MarshalInterceptor)
	interceptors[field] = func() ([]byte, error) {
		return c.TabularJSON.OutputMarshaller.Marshal(data)
	}
	return interceptors
}
