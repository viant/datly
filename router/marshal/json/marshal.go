package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
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

func (j *Marshaller) Marshal(value interface{}, options ...interface{}) ([]byte, error) {
	if value == nil {
		return []byte(null), nil
	}

	rType := reflect.TypeOf(value)
	marshaller, err := j.marshaller(rType)
	if err != nil {
		return nil, err
	}

	session, putBufferBack := j.PrepareSession(options)
	if putBufferBack {
		defer bufferPool.Put(session.Buffer)
	}

	pointer := AsPtr(value, rType)

	if err = marshaller.MarshallObject(pointer, session); err != nil {
		return nil, err
	}

	output := make([]byte, len(session.Buffer.Bytes()))
	copy(output, session.Bytes())

	return output, nil
}

func (j *Marshaller) PrepareSession(options []interface{}) (*Session, bool) {
	if len(options) == 0 {
		return &Session{
			Buffer: bufferPool.Get(),
		}, true
	}

	var session *Session
	var filters *Filters
	var putBufferBack bool

	for _, option := range options {
		if option == nil {
			continue
		}

		switch actual := option.(type) {
		case *Session:
			session = actual
			putBufferBack = session.Buffer == nil
		}
	}

	if session == nil {
		session = &Session{
			Options: options,
			Buffer:  bufferPool.Get(),
		}

		putBufferBack = true
	}

	if session.Filters == nil {
		session.Filters = filters
	}

	return session, putBufferBack
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}, options ...interface{}) error {
	rType := reflect.TypeOf(dest).Elem()

	marshaler, err := j.marshaller(rType)
	if err != nil {
		return err
	}

	aDecoder := gojay.BorrowDecoder(bytes.NewReader(data))
	defer aDecoder.Release()
	pointer := xunsafe.AsPointer(dest)

	result := marshaler.UnmarshallObject(pointer, aDecoder, nil)

	return result
}

func AsPtr(dest interface{}, rType reflect.Type) unsafe.Pointer {
	switch rType.Kind() {
	case reflect.Interface:
		return unsafe.Pointer(&dest)
	case reflect.Ptr:
		return xunsafe.RefPointer(xunsafe.AsPointer(dest))
	default:
		return xunsafe.AsPointer(dest)
	}
}

func EnsureType(rType reflect.Type, ptr unsafe.Pointer) reflect.Type {
	inlinableType := rType
	if inlinableType.Kind() == reflect.Interface {
		inlinableType = reflect.TypeOf(xunsafe.AsInterface(ptr))
	}
	return inlinableType
}

func (j *Marshaller) marshaller(rType reflect.Type) (Marshaler, error) {
	return j.cache.LoadMarshaller(rType, j.config, "", "", nil)
}
