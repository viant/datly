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
		cache  *marshallersCache
		config marshal.Default
	}
)

func New(config marshal.Default) (*Marshaller, error) {
	m := &Marshaller{
		cache:  newCache(),
		config: config,
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

	session, putBufferBack := j.prepareMarshallSession(options)
	if putBufferBack {
		defer buffersPool.put(session.Buffer)
	}

	pointer := AsPtr(value, rType)

	if err = marshaller.MarshallObject(pointer, session); err != nil {
		return nil, err
	}

	output := make([]byte, len(session.Buffer.Bytes()))
	copy(output, session.Bytes())

	return output, nil
}

func (j *Marshaller) prepareMarshallSession(options []interface{}) (*MarshallSession, bool) {
	if len(options) == 0 {
		return &MarshallSession{
			Buffer: buffersPool.get(),
		}, true
	}

	var session *MarshallSession
	var filters *Filters
	var putBufferBack bool

	for _, option := range options {
		if option == nil {
			continue
		}

		switch actual := option.(type) {
		case *MarshallSession:
			session = actual
			putBufferBack = session.Buffer == nil
		}
	}

	if session == nil {
		session = &MarshallSession{
			Options: options,
			Buffer:  buffersPool.get(),
		}

		putBufferBack = true
	}

	if session.Filters == nil {
		session.Filters = filters
	}

	return session, putBufferBack
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}, options ...interface{}) error {
	rType := reflect.TypeOf(dest)

	marshaler, err := j.marshaller(rType)
	if err != nil {
		return err
	}

	aDecoder := gojay.BorrowDecoder(bytes.NewReader(data))
	defer aDecoder.Release()

	result := marshaler.UnmarshallObject(AsPtr(dest, rType), aDecoder, nil, j.prepareUnmarshallSession(options))
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
	return j.cache.loadMarshaller(rType, j.config, "", "", nil)
}

func (j *Marshaller) prepareUnmarshallSession(options []interface{}) *UnmarshallSession {
	var unmarshallSession *UnmarshallSession
	var interceptors Interceptors
	for _, option := range options {
		switch actual := option.(type) {
		case *UnmarshallSession:
			unmarshallSession = actual
		case Interceptors:
			interceptors = actual
		}
	}

	if unmarshallSession == nil {
		unmarshallSession = &UnmarshallSession{}
	}

	if len(unmarshallSession.Interceptors) == 0 {
		unmarshallSession.Interceptors = interceptors
	}

	unmarshallSession.Options = append(unmarshallSession.Options, options...)

	return unmarshallSession
}
