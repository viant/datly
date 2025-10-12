package json

import (
	"fmt"
	"unsafe"

	"github.com/francoispqt/gojay"
)

// deferredMarshaller is a placeholder used to break recursive type graphs during construction.
// It forwards calls to the actual target once it is set.
type deferredMarshaller struct {
	target marshaler
	ready  chan struct{}
	err    error
}

func newDeferred() *deferredMarshaller {
	return &deferredMarshaller{ready: make(chan struct{})}
}

func (d *deferredMarshaller) setTarget(m marshaler) {
	d.target = m
	close(d.ready)
}

func (d *deferredMarshaller) fail(e error) {
	d.err = e
	close(d.ready) // writes to err happen-before any receive on ready
}

func (d *deferredMarshaller) resolved() (marshaler, error) {
	<-d.ready // wait for resolve/fail
	if d.err != nil {
		return nil, d.err
	}
	if d.target == nil {
		return nil, fmt.Errorf("marshaller not initialized")
	}
	return d.target, nil
}

func (d *deferredMarshaller) MarshallObject(ptr unsafe.Pointer, s *MarshallSession) error {
	m, err := d.resolved()
	if err != nil {
		return err
	}
	return m.MarshallObject(ptr, s)
}

func (d *deferredMarshaller) UnmarshallObject(p unsafe.Pointer, dec, aux *gojay.Decoder, s *UnmarshalSession) error {
	m, err := d.resolved()
	if err != nil {
		return err
	}
	return m.UnmarshallObject(p, dec, aux, s)
}
