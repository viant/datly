package content

import (
	"fmt"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/gateway/router/marshal/config"
	"github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/gateway/router/marshal/tabjson"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/structology/format"
	"github.com/viant/xlsy"
	"github.com/viant/xmlify"
	"net/http"
	"reflect"
	"strings"
)

const (
	HeaderContentType = "Content-Type"
)

type (
	Content struct {
		Marshaller
		DateFormat               string             `json:",omitempty"`
		CSV                      *CSVConfig         `json:",omitempty"`
		XLS                      *XLSConfig         `json:",omitempty"`
		XML                      *XMLConfig         `json:",omitempty"`
		TabularJSON              *TabularJSONConfig `json:",omitempty"`
		Transforms               marshal.Transforms `json:"Transforms,omitempty" yaml:"Transforms,omitempty" `
		unmarshallerInterceptors marshal.Transforms
	}
	XLSConfig struct {
		DefaultStyle string
		SheetName    string
		Styles       map[string]string //name of style, values
	}

	TabularJSONConfig struct {
		FloatPrecision   string
		_config          *tabjson.Config
		InputMarhsaller  *tabjson.Marshaller
		OutputMarshaller *tabjson.Marshaller
	}

	XMLConfig struct {
		FloatPrecision   string
		config           *xmlify.Config
		InputMarshaller  *xmlify.Marshaller
		OutputMarshaller *xmlify.Marshaller
	}

	JSON struct {
		JsonMarshaller *json.Marshaller
	}

	XLS struct {
		XlsMarshaller *xlsy.Marshaller
	}
	Marshaller struct {
		XLS
		JSON
	}
)

func (c *Content) UnmarshallerInterceptors() marshal.Transforms {
	return c.unmarshallerInterceptors
}

func (c *Content) UnmarshalFunc(request *http.Request) shared.Unmarshal {
	contentType := request.Header.Get(HeaderContentType)
	setter.SetStringIfEmpty(&contentType, request.Header.Get(strings.ToLower(HeaderContentType)))
	switch contentType {
	case CSVContentType:
		return c.CSV.Unmarshal
	}
	return func(bytes []byte, i interface{}) error {
		return c.JsonMarshaller.Unmarshal(bytes, i, request)
	}
}

func (x *XLSConfig) Options() []xlsy.Option {

	var options []xlsy.Option
	if x == nil {
		return options
	}
	if x.DefaultStyle != "" {
		options = append(options, xlsy.WithDefaultStyle(x.DefaultStyle))
	}
	if x.SheetName != "" {
		options = append(options, xlsy.WithTag(&xlsy.Tag{Tag: &format.Tag{Name: x.SheetName}}))
	}
	if len(x.Styles) > 0 {
		var pairs []string
		for k, v := range x.Styles {
			pairs = append(pairs, k, v)
		}
		options = append(options, xlsy.WithNamedStyles(pairs...))
	}
	return options
}

func (c *Content) InitMarshaller(config *config.IOConfig, exclude []string, inputType, outputType reflect.Type) error {
	c.unmarshallerInterceptors = c.Transforms.FilterByKind(marshal.TransformKindUnmarshal)
	c.JsonMarshaller = json.New(config)
	c.XlsMarshaller = xlsy.NewMarshaller(c.XLS.Options()...)

	if err := c.initCSVIfNeeded(inputType, outputType); err != nil {
		return err
	}
	if err := c.initTabJSONIfNeeded(exclude, inputType, outputType); err != nil {
		return err
	}
	if err := c.initXMLIfNeeded(exclude, inputType, outputType); err != nil {
		return err
	}
	return nil
}

func (c *Content) initCSVIfNeeded(inputType reflect.Type, outputType reflect.Type) error {
	c.ensureCSV()
	if len(c.CSV.Separator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", c.CSV.Separator)
	}
	if c.CSV.NullValue == "" {
		c.CSV.NullValue = "null"
	}
	c.CSV._config = &csv.Config{
		FieldSeparator:  c.CSV.Separator,
		ObjectSeparator: "\n",
		EncloseBy:       `"`,
		EscapeBy:        "\\",
		NullValue:       c.CSV.NullValue,
	}
	schemaType := inputType
	if schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}
	var err error
	c.CSV.OutputMarshaller, err = csv.NewMarshaller(schemaType, c.CSV._config)
	if err != nil {
		return err
	}
	if outputType == nil {
		return nil
	}

	//c.CSV._unwrapperSlice = c._requestBodySlice
	c.CSV.InputMarshaller, err = csv.NewMarshaller(outputType, nil)
	return err
}

func (c *Content) ensureCSV() {
	if c.CSV != nil {
		return
	}
	c.CSV = &CSVConfig{Separator: ","}
}

func (c *Content) initTabJSONIfNeeded(excludedPaths []string, inputType reflect.Type, outputType reflect.Type) error {

	if c.TabularJSON == nil {
		c.TabularJSON = &TabularJSONConfig{}
	}

	if c.TabularJSON._config == nil {
		c.TabularJSON._config = &tabjson.Config{}
	}

	if c.TabularJSON._config.FieldSeparator == "" {
		c.TabularJSON._config.FieldSeparator = ","
	}

	if len(c.TabularJSON._config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", c.TabularJSON._config.FieldSeparator)
	}

	if c.TabularJSON._config.NullValue == "" {
		c.TabularJSON._config.NullValue = "null"
	}

	if c.TabularJSON.FloatPrecision != "" {
		c.TabularJSON._config.StringifierConfig.StringifierFloat32Config.Precision = c.TabularJSON.FloatPrecision
		c.TabularJSON._config.StringifierConfig.StringifierFloat64Config.Precision = c.TabularJSON.FloatPrecision
	}

	c.TabularJSON._config.ExcludedPaths = excludedPaths

	if outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}

	var err error
	c.TabularJSON.OutputMarshaller, err = tabjson.NewMarshaller(outputType, c.TabularJSON._config)
	if err != nil {
		return err
	}

	if outputType == nil {
		return nil
	}
	c.TabularJSON.InputMarhsaller, err = tabjson.NewMarshaller(inputType, nil)
	return err
}

// func (c *Content) initXMLIfNeeded(excludedPaths []string, outputType reflect.Type, inputType reflect.Type) error {
func (c *Content) initXMLIfNeeded(excludedPaths []string, inputType reflect.Type, outputType reflect.Type) error {
	if c.XML == nil {
		c.XML = &XMLConfig{}
	}
	if c.XML.config == nil {
		c.XML.config = getDefaultConfig()
	}

	if c.XML.config.FieldSeparator == "" {
		c.XML.config.FieldSeparator = ","
	}

	if len(c.XML.config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", c.XML.config.FieldSeparator)
	}

	if c.XML.config.NullValue == "" {
		c.XML.config.NullValue = "\u0000"
	}

	if c.XML.FloatPrecision != "" {
		c.XML.config.StringifierConfig.StringifierFloat32Config.Precision = c.XML.FloatPrecision
		c.XML.config.StringifierConfig.StringifierFloat64Config.Precision = c.XML.FloatPrecision
	}
	c.XML.config.ExcludedPaths = excludedPaths

	if outputType == nil {
		return nil
	}

	if outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}

	var err error
	c.XML.OutputMarshaller, err = xmlify.NewMarshaller(outputType, c.XML.config)
	if err != nil {
		return err
	}

	if inputType == nil {
		return nil
	}
	c.XML.InputMarshaller, err = xmlify.NewMarshaller(inputType, nil)
	return err
}

// TODO MFI
func getDefaultConfig() *xmlify.Config {
	return &xmlify.Config{
		Style:                  "regularStyle", // style
		RootTag:                "result",
		HeaderTag:              "columns",
		HeaderRowTag:           "column",
		HeaderRowFieldAttr:     "id",
		HeaderRowFieldTypeAttr: "type",
		DataTag:                "rows",
		DataRowTag:             "r",
		DataRowFieldTag:        "c",
		NewLineSeparator:       "\n",
		DataRowFieldTypes: map[string]string{
			"uint":    "lg",
			"uint8":   "lg",
			"uint16":  "lg",
			"uint32":  "lg",
			"uint64":  "lg",
			"int":     "lg",
			"int8":    "lg",
			"int16":   "lg",
			"int32":   "lg",
			"int64":   "lg",
			"*uint":   "lg",
			"*uint8":  "lg",
			"*uint16": "lg",
			"*uint32": "lg",
			"*uint64": "lg",
			"*int":    "lg",
			"*int8":   "lg",
			"*int16":  "lg",
			"*int32":  "lg",
			"*int64":  "lg",
			/////
			"float32": "db",
			"float64": "db",
			/////
			"string":  "string",
			"*string": "string",
			//////
			"time.Time":  "dt",
			"*time.Time": "dt",
		},
		HeaderRowFieldType: map[string]string{
			"uint":    "long",
			"uint8":   "long",
			"uint16":  "long",
			"uint32":  "long",
			"uint64":  "long",
			"int":     "long",
			"int8":    "long",
			"int16":   "long",
			"int32":   "long",
			"int64":   "long",
			"*uint":   "long",
			"*uint8":  "long",
			"*uint16": "long",
			"*uint32": "long",
			"*uint64": "long",
			"*int":    "long",
			"*int8":   "long",
			"*int16":  "long",
			"*int32":  "long",
			"*int64":  "long",
			/////
			"float32": "double",
			"float64": "double",
			/////
			"string":  "string",
			"*string": "string",
			//////
			"time.Time":  "date",
			"*time.Time": "date",
		},
		TabularNullValue: "nil=\"true\"",
		//RegularRootTag:   "root",
		RegularRootTag:   "forecast-response",
		RegularRowTag:    "row",
		RegularNullValue: "",
		NullValue:        "\u0000",
	}
}

func (c *CSVConfig) Unmarshal(bytes []byte, i interface{}) error {
	return c.InputMarshaller.Unmarshal(bytes, i)
}

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
