package auth

import (
	"reflect"

	"github.com/viant/datly/transcribe/testdata/linkedimports/status"
	"github.com/viant/xdatly"
)

// Deliberately lacks a route. Loading imported types must not compile this holder.
type Components struct {
	Auth xdatly.Component[struct{}, Output] `component:"Unrelated,handler=NewHandler"`
}

func NewHandler() any { panic("type discovery must not invoke factories") }

// Keep compiled identities linked without registering contracts with Datly.
var LinkedTypes = []reflect.Type{
	reflect.TypeOf((*Components)(nil)),
	reflect.TypeOf((*Output)(nil)),
	reflect.TypeOf((*status.Status)(nil)),
}
