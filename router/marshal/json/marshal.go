package json

import (
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
	_, err := m.cache.LoadMarshaller(rType, config, "", "", &DefaultTag{})
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (j *Marshaller) Marshal(value interface{}, filters *Filters) ([]byte, error) {
	if value == nil {
		return []byte(null), nil
	}
	rType := reflect.TypeOf(value)
	marshaller, err := j.cache.LoadMarshaller(rType, j.config, "", "", &DefaultTag{})
	if err != nil {
		return nil, err
	}

	buffer := bufferPool.Get()
	if err = marshaller.MarshallObject(rType, xunsafe.AsPointer(value), buffer, filters); err != nil {
		return nil, err
	}

	output := make([]byte, len(buffer.Bytes()))
	copy(output, buffer.Bytes())
	bufferPool.Put(buffer)

	return output, nil
}
