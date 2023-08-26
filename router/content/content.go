package content

import (
	"fmt"
	"github.com/viant/datly/router/marshal/common"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/router/marshal/tabjson"
	"github.com/viant/sqlx/io/load/reader/csv"
	"github.com/viant/xlsy"
	"github.com/viant/xmlify"
	"reflect"
)

type (
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

	Content struct {
		Marshaller
		DateFormat  string             `json:",omitempty"`
		CSV         *CSVConfig         `json:",omitempty"`
		XLS         *XLSConfig         `json:",omitempty"`
		XML         *XMLConfig         `json:",omitempty"`
		TabularJSON *TabularJSONConfig `json:",omitempty"`
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

func (x *XLSConfig) Options() []xlsy.Option {

	var options []xlsy.Option
	if x == nil {
		return options
	}
	if x.DefaultStyle != "" {
		options = append(options, xlsy.WithDefaultStyle(x.DefaultStyle))
	}
	if x.SheetName != "" {
		options = append(options, xlsy.WithTag(&xlsy.Tag{Name: x.SheetName}))
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

func (r *Content) InitMarshaller(config common.IOConfig, exclude []string, inputType, outputType reflect.Type) error {
	r.JsonMarshaller = json.New(config)
	r.XlsMarshaller = xlsy.NewMarshaller(r.XLS.Options()...)

	if err := r.initCSVIfNeeded(inputType, outputType); err != nil {
		return err
	}
	if err := r.initTabJSONIfNeeded(exclude, inputType, outputType); err != nil {
		return err
	}
	if err := r.initXMLIfNeeded(exclude, inputType, outputType); err != nil {
		return err
	}
	return nil
}

func (r *Content) initCSVIfNeeded(inputType reflect.Type, outputType reflect.Type) error {
	r.ensureCSV()
	if len(r.CSV.Separator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.CSV.Separator)
	}
	if r.CSV.NullValue == "" {
		r.CSV.NullValue = "null"
	}
	r.CSV._config = &csv.Config{
		FieldSeparator:  r.CSV.Separator,
		ObjectSeparator: "\n",
		EncloseBy:       `"`,
		EscapeBy:        "\\",
		NullValue:       r.CSV.NullValue,
	}
	schemaType := inputType
	if schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}
	var err error
	r.CSV.OutputMarshaller, err = csv.NewMarshaller(schemaType, r.CSV._config)
	if err != nil {
		return err
	}
	if outputType == nil {
		return nil
	}

	//r.CSV._unwrapperSlice = r._requestBodySlice
	r.CSV.InputMarshaller, err = csv.NewMarshaller(outputType, nil)
	return err
}

func (r *Content) ensureCSV() {
	if r.CSV != nil {
		return
	}
	r.CSV = &CSVConfig{Separator: ","}
}

func (r *Content) initTabJSONIfNeeded(excludedPaths []string, inputType reflect.Type, outputType reflect.Type) error {

	if r.TabularJSON == nil {
		r.TabularJSON = &TabularJSONConfig{}
	}

	if r.TabularJSON._config == nil {
		r.TabularJSON._config = &tabjson.Config{}
	}

	if r.TabularJSON._config.FieldSeparator == "" {
		r.TabularJSON._config.FieldSeparator = ","
	}

	if len(r.TabularJSON._config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.TabularJSON._config.FieldSeparator)
	}

	if r.TabularJSON._config.NullValue == "" {
		r.TabularJSON._config.NullValue = "null"
	}

	if r.TabularJSON.FloatPrecision != "" {
		r.TabularJSON._config.StringifierConfig.StringifierFloat32Config.Precision = r.TabularJSON.FloatPrecision
		r.TabularJSON._config.StringifierConfig.StringifierFloat64Config.Precision = r.TabularJSON.FloatPrecision
	}

	r.TabularJSON._config.ExcludedPaths = excludedPaths

	if inputType.Kind() == reflect.Ptr {
		inputType = inputType.Elem()
	}

	var err error
	r.TabularJSON.OutputMarshaller, err = tabjson.NewMarshaller(inputType, r.TabularJSON._config)
	if err != nil {
		return err
	}

	if outputType == nil {
		return nil
	}
	r.TabularJSON.InputMarhsaller, err = tabjson.NewMarshaller(outputType, nil)
	return err
}

func (r *Content) initXMLIfNeeded(excludedPaths []string, outputType reflect.Type, inputType reflect.Type) error {
	if r.XML == nil {
		r.XML = &XMLConfig{}
	}
	if r.XML.config == nil {
		r.XML.config = getDefaultConfig()
	}

	if r.XML.config.FieldSeparator == "" {
		r.XML.config.FieldSeparator = ","
	}

	if len(r.XML.config.FieldSeparator) != 1 {
		return fmt.Errorf("separator has to be a single char, but was %v", r.XML.config.FieldSeparator)
	}

	if r.XML.config.NullValue == "" {
		r.XML.config.NullValue = "\u0000"
	}

	if r.XML.FloatPrecision != "" {
		r.XML.config.StringifierConfig.StringifierFloat32Config.Precision = r.XML.FloatPrecision
		r.XML.config.StringifierConfig.StringifierFloat64Config.Precision = r.XML.FloatPrecision
	}
	r.XML.config.ExcludedPaths = excludedPaths
	if outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}

	var err error
	r.XML.OutputMarshaller, err = xmlify.NewMarshaller(outputType, r.XML.config)
	if err != nil {
		return err
	}

	if inputType == nil {
		return nil
	}
	r.XML.InputMarshaller, err = xmlify.NewMarshaller(inputType, nil)
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
		NewLine:                "\n",
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
		RegularRootTag:   "root",
		RegularRowTag:    "row",
		RegularNullValue: "",
		NullValue:        "\u0000",
	}
}

func (c *CSVConfig) Unmarshal(bytes []byte, i interface{}) error {
	return c.InputMarshaller.Unmarshal(bytes, i)
}
