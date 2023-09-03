package tabjson

import (
	"fmt"
	"github.com/viant/sqlx/io"
	"github.com/viant/toolbox/format"
	goIo "io"
	"reflect"
	"strings"
)

// Reader represents plain text reader
type (
	Reader struct {
		config              *Config
		valueAt             func(index int) interface{}
		stringifier         io.ObjectStringifierFn
		itemBuffer          *Buffer
		itemCount           int
		index               int
		offsetOfCurrentRead int
		isEOF               bool
		initialized         bool
		stringifierConfig   *io.StringifierConfig
		objectStringifier   *io.ObjectStringifier
		objectWritten       bool
	}
)

// Read data into itemBuffer
func (r *Reader) Read(buffer []byte) (n int, err error) {
	if (r.isEOF || r.index >= r.itemCount) && r.itemBuffer.len() == 0 {
		return 0, goIo.EOF
	}

	r.offsetOfCurrentRead = 0

	if err = r.init(); err != nil {
		return 0, err
	}

	for {
		if r.itemBuffer.len() > 0 {
			n, err = r.itemBuffer.Read(buffer[r.offsetOfCurrentRead:])
			if err == goIo.EOF {
				r.itemBuffer.reset()
			}

			r.offsetOfCurrentRead += n

			if r.offsetOfCurrentRead == len(buffer) {
				return r.offsetOfCurrentRead, nil
			}
		} else {
			if r.index >= r.itemCount {
				r.isEOF = true
				return r.offsetOfCurrentRead, nil
			}
			r.fillItemBuffer(r.index)
			r.index++
		}
	}
}

func (r *Reader) init() error {
	if r.initialized {
		return nil
	}

	if err := r.writeHeaderIfNeeded(); err != nil {
		return err
	}

	r.initialized = true
	return nil
}

// fillItemBuffer stringifies and reads data into r.itemBuffer, separates objects and fields values with given separators.
func (r *Reader) fillItemBuffer(i int) {
	record := r.valueAt(i)
	stringifiedFieldValues, wasString := r.stringifier(record)

	r.writeObject(stringifiedFieldValues, wasString)
	r.itemBuffer.offset = 0
}

func (r *Reader) writeObjectSeparator() {
	r.itemBuffer.writeString(r.config.ObjectSeparator)
}

func (r *Reader) writeObject(stringifiedFieldValues []string, wasString []bool) {
	if r.objectWritten {
		r.writeObjectSeparator()
	}

	WriteObject(r.itemBuffer, r.config, stringifiedFieldValues, wasString)
	r.objectWritten = true
}

func WriteObject(writer *Buffer, config *Config, values []string, wasString []bool) {
	if len(values) == 0 {
		return
	}

	//writer.writeString("[")

	for j := 0; j < len(values); j++ {
		if j != 0 {
			writer.writeString(config.FieldSeparator) //TODO MFI field separtator always
		}

		asString := EscapeSpecialChars(values[j], config) //TODO MFI escaping
		if wasString[j] {
			asString = config.EncloseBy + asString + config.EncloseBy
		}

		writer.writeString(asString)
	}

	//writer.writeString("]")

}

func EscapeSpecialChars(value string, config *Config) string {
	value = strings.ReplaceAll(value, config.EscapeBy, config.EscapeBy+config.EscapeBy)

	if !config.Stringify.IgnoreFieldSeparator {
		value = strings.ReplaceAll(value, config.FieldSeparator, config.EscapeBy+config.FieldSeparator)
	}
	if !config.Stringify.IgnoreObjectSeparator {
		value = strings.ReplaceAll(value, config.ObjectSeparator, config.EscapeBy+config.ObjectSeparator)
	}
	if !config.Stringify.IgnoreEncloseBy {
		value = strings.ReplaceAll(value, config.EncloseBy, config.EscapeBy+config.EncloseBy)
	}
	return value
}

// NewReader returns Reader instance and actual data struct type.
// e.g. data is type of []*Foo - returns Foo.
func NewReader(any interface{}, config *Config, options ...interface{}) (*Reader, reflect.Type, error) {
	valueAt, size, err := io.Values(any)
	if err != nil {
		return nil, nil, err
	}

	structType := io.EnsureDereference(valueAt(0))
	stringifier, stringifierConfig := readOptions(options)
	if stringifier == nil {
		stringifier = io.TypeStringifier(structType, config.NullValue, true)
	}

	stringifierFn, err := stringifier.Stringifier(options...)

	if err != nil {
		return nil, nil, err
	}

	r := &Reader{
		objectStringifier: stringifier,
		config:            config,
		valueAt:           valueAt,
		itemCount:         size,
		stringifier:       stringifierFn,
		itemBuffer:        NewBuffer(1024),
		stringifierConfig: stringifierConfig,
	}

	return r, structType, nil
}

func readOptions(options []interface{}) (*io.ObjectStringifier, *io.StringifierConfig) {
	var stringifier *io.ObjectStringifier
	var stringifierConfig *io.StringifierConfig

	for _, option := range options {
		switch actual := option.(type) {
		case io.ObjectStringifier:
			stringifier = &actual
		case *io.ObjectStringifier:
			stringifier = actual
		case *io.StringifierConfig:
			stringifierConfig = actual
		case io.StringifierConfig:
			stringifierConfig = &actual
		}
	}

	return stringifier, stringifierConfig
}

// ItemCount returns count of items inside itemBuffer
func (r *Reader) ItemCount() int {
	return r.itemCount
}

func (r *Reader) writeHeaderIfNeeded() error {
	if r.stringifierConfig == nil {
		return nil
	}

	fields, err := r.fields()
	if err != nil {
		return err
	}

	if r.stringifierConfig.CaseFormat != format.CaseUpperCamel {
		for i, field := range fields {
			fields[i] = format.CaseUpperCamel.Format(field, r.stringifierConfig.CaseFormat)
		}
	}

	wasStrings := make([]bool, len(fields))
	for i := range wasStrings {
		wasStrings[i] = true
	}

	r.writeObject(fields, wasStrings)
	return nil
}

func (r *Reader) fields() ([]string, error) {
	fieldsLen := len(r.stringifierConfig.Fields)
	if fieldsLen == 0 {
		return r.objectStringifier.FieldNames(), nil
	}

	fields := make([]string, 0, fieldsLen)
	for i, field := range r.stringifierConfig.Fields {
		ok := r.objectStringifier.Has(field)
		if !ok {
			return nil, fmt.Errorf("not found field %v", field)
		}

		fields = append(fields, r.stringifierConfig.Fields[i])
	}

	return fields, nil
}
