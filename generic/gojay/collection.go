package gojay

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/datly/generic"
)

type Collection struct {
	Collection generic.Collection
}

func (c Collection) IsNil() bool {
	if c.Collection == nil {
		return true
	}
	return c.Collection.Size() == 0
}

func (c Collection) MarshalJSONArray(enc *gojay.Encoder) {
	if c.Collection == nil {
		return
	}
	c.Collection.Objects(func(item *generic.Object) (b bool, err error) {
		enc.AddObject(&Object{item})
		return true, nil
	})
}
