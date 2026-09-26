package output

import (
	"fmt"
	"reflect"
	"strings"

	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	xresponse "github.com/viant/xdatly/response"
)

// WireContract describes a statically representable encoded result. Type is the
// wire value type, which may differ from the component's structural output type.
// Binary marks an opaque byte stream rather than a textual string.
type WireContract struct {
	JSON               *jsonmarshal.WireShape
	ContentDisposition string
	Type               reflect.Type
	ContentType        string
	Binary             bool
}

// JSONWireType is an optional declaration for custom JSON marshalers whose
// successful wire value has a stable, separately typed shape. OpenAPI uses the
// declared type instead of treating the marshaler as arbitrary JSON.
type JSONWireType interface {
	DatlyJSONWireType() reflect.Type
}

// TransportReady reports the declared SDK response contract, independently of
// any invocation result. Documentation may supply static HTTP response schemas.
func (p *Plan) TransportReady() bool {
	return p != nil && p.typeOf != nil && (p.typeOf.Implements(reflect.TypeFor[xresponse.Response]()) || reflect.PointerTo(p.typeOf).Implements(reflect.TypeFor[xresponse.Response]()))
}

// Wire derives representability from the actual compiled encoding plan. Source
// settings may be changed or absent after compilation and are not authority.
// Transformed JSON uses the native serializer plan. XML/tabular and opaque
// encoders still require an explicit wire projection.
func (p *Plan) Wire(format string) (WireContract, error) {
	if p == nil || p.typeOf == nil {
		return WireContract{}, fmt.Errorf("compiled output type is required")
	}
	if p.typeOf.Implements(reflect.TypeFor[xresponse.Response]()) || reflect.PointerTo(p.typeOf).Implements(reflect.TypeFor[xresponse.Response]()) {
		return WireContract{}, fmt.Errorf("explicit Response has dynamic status/media/body without compiled wire authority")
	}
	if format == "" {
		format = p.DefaultFormat()
	}
	format = strings.ToLower(strings.TrimSpace(format))
	contentType, err := ContentType(format)
	if err != nil {
		return WireContract{}, err
	}
	result := WireContract{ContentType: contentType}
	switch format {
	case "json":
		if p.custom != nil {
			return WireContract{}, fmt.Errorf("output presentation requires a wire schema projection from the compiled output owner")
		}
		result.Type = p.typeOf
		if p.jsonEncoder != nil {
			result.JSON, err = p.jsonEncoder.Wire()
		} else if p.standardJSON != nil {
			result.JSON, err = p.standardJSON.Wire()
		}
		if err != nil {
			return WireContract{}, err
		}
		candidate := reflect.New(p.typeOf)
		if p.typeOf.Kind() == reflect.Pointer {
			candidate = reflect.New(p.typeOf.Elem())
		}
		if declared, ok := candidate.Interface().(JSONWireType); ok {
			wireType := declared.DatlyJSONWireType()
			if wireType == nil {
				return WireContract{}, fmt.Errorf("custom JSON wire type is nil")
			}
			result.JSON = nil
			result.Type = wireType
		}
	case "csv":
		if p.rows == nil {
			return WireContract{}, fmt.Errorf("CSV output requires typed rows")
		}
		result.Type = reflect.TypeFor[string]()
	case "xls", "xlsx":
		result.Type = reflect.TypeFor[string]()
		result.Binary = true
	default:
		return WireContract{}, fmt.Errorf("output format %q needs a compiled wire schema projection", format)
	}
	result.ContentDisposition = p.disposition(format)
	return result, nil
}
