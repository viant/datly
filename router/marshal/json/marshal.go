package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
)

const null = `null`

var nullBytes = []byte(`null`)

const defaultCaser = format.CaseUpperCamel
const IndexKey = "presenceIndex"

type (
	Marshaller struct {
		cache  *Cache
		config marshal.Default
	}
)

func New(rType reflect.Type, config marshal.Default) (*Marshaller, error) {
	m := &Marshaller{
		cache:  NewCache(),
		config: marshal.Default{},
	}
	_, err := m.cache.LoadMarshaller(rType, config, "", "", nil)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (j *Marshaller) Marshal(value interface{}, filters *Filters, options ...interface{}) ([]byte, error) {
	if value == nil {
		return []byte(null), nil
	}
	rType := reflect.TypeOf(value)
	marshaller, err := j.marshaller(rType)
	if err != nil {
		return nil, err
	}

	buffer := bufferPool.Get()

	session := &Session{
		Filters: filters,
		Buffer:  buffer,
	}

	if err = marshaller.MarshallObject(rType, xunsafe.AsPointer(value), session); err != nil {
		return nil, err
	}

	output := make([]byte, len(buffer.Bytes()))
	copy(output, buffer.Bytes())
	bufferPool.Put(buffer)

	return output, nil
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}, options ...interface{}) error {
	rType := reflect.TypeOf(dest).Elem()

	marshaler, err := j.marshaller(rType)
	if err != nil {
		return err
	}

	aDecoder := gojay.BorrowDecoder(bytes.NewReader(data))
	defer aDecoder.Release()
	return marshaler.UnmarshallObject(rType, xunsafe.AsPointer(dest), aDecoder, nil)
}

func (j *Marshaller) marshaller(rType reflect.Type) (Marshaler, error) {
	return j.cache.ElemMarshallerIfNeeded(rType, j.config, "", "", nil)
}
