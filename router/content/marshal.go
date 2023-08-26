package content

import (
	"fmt"
	"github.com/viant/datly/router/marshal/json"
	"strings"
)

// TODO refactor it to take just a marshaller for format, marshaller should only deal with response
// (requiring field, reader data seems code design issue)
func (c *Content) Marshal(format string, field string, readerData, response interface{}, options ...interface{}) ([]byte, error) {
	switch strings.ToLower(format) {
	case XLSFormat:
		return c.Marshaller.XLS.XlsMarshaller.Marshal(response)
	case CSVFormat:
		return c.CSV.OutputMarshaller.Marshal(readerData)
	case XMLFormat:
		return c.XML.OutputMarshaller.Marshal(response)
	case JSONDataFormatTabular:
		if field != "" {
			tabJSONInterceptors := c.tabJSONInterceptors(field, readerData)
			return c.JsonMarshaller.Marshal(response, tabJSONInterceptors)
		}
		return c.TabularJSON.OutputMarshaller.Marshal(readerData)
	case JSONFormat:
		return c.JsonMarshaller.Marshal(response, options...)
	default:
		return nil, fmt.Errorf("unsupproted readerData format: %s", format)
	}
}

func (c *Content) tabJSONInterceptors(field string, data interface{}) json.MarshalerInterceptors {
	interceptors := make(map[string]json.MarshalInterceptor)
	interceptors[field] = func() ([]byte, error) {
		return c.TabularJSON.OutputMarshaller.Marshal(data)
	}
	return interceptors
}
