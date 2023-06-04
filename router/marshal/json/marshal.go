package json

import (
	"bytes"
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/router/marshal/default"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

const null = `null`

var nullBytes = []byte(`null`)

const defaultCaser = format.CaseUpperCamel
const IndexKey = "setMarker"

type (
	Marshaller struct {
		cache  *marshallersCache
		config _default.Default
	}
)

func New(config _default.Default) (*Marshaller, error) {
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
	// TODO MFI WHY FILTERS ARE NIL AND WE SET NIL AGAIN?
	if session.Filters == nil {
		session.Filters = filters
	}

	for _, option := range options {
		if option == nil {
			continue
		}

		switch actual := option.(type) {
		case MarshalerInterceptors:
			session.Interceptors = actual
		}
	}

	if session.Interceptors == nil {
		session.Interceptors = make(map[string]MarshalInterceptor)
	}

	return session, putBufferBack
}

func (j *Marshaller) Unmarshal(data []byte, dest interface{}, options ...interface{}) error {
	rType := reflect.TypeOf(dest)

	aMarshaler, err := j.marshaller(rType)
	if err != nil {
		return err
	}

	aDecoder := gojay.BorrowDecoder(bytes.NewReader(data))
	defer aDecoder.Release()

	result := aMarshaler.UnmarshallObject(AsPtr(dest, rType), aDecoder, nil, j.prepareUnmarshallSession(options))
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

func (j *Marshaller) marshaller(rType reflect.Type) (marshaler, error) {
	return j.cache.loadMarshaller(rType, j.config, "", "", nil)
}

func (j *Marshaller) prepareUnmarshallSession(options []interface{}) *UnmarshalSession {
	var unmarshallSession *UnmarshalSession
	var interceptors UnmarshalerInterceptors
	for _, option := range options {
		switch actual := option.(type) {
		case *UnmarshalSession:
			unmarshallSession = actual
		case UnmarshalerInterceptors:
			interceptors = actual
		}
	}

	if unmarshallSession == nil {
		unmarshallSession = &UnmarshalSession{}
	}

	if len(unmarshallSession.PathMarshaller) == 0 {
		unmarshallSession.PathMarshaller = interceptors
	}

	unmarshallSession.Options = append(unmarshallSession.Options, options...)

	return unmarshallSession
}
