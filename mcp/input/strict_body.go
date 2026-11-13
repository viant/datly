package input

import (
	bodyprovider "github.com/viant/bindly/provider/body"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/x/shape"
)

// Prepare the existing native decoder policy once. MCP strict admission shares
// the compiled tool option; ordinary HTTP and lenient tools keep their decoder.
var strictBodyDecoders = func() *bodyprovider.DecoderRegistry {
	registry := bodyprovider.NewDecoderRegistry()
	registry.Register("application/json", strictBodyDecoder{})
	return registry
}()

type strictBodyDecoder struct{}

func (strictBodyDecoder) Decode(raw []byte, target any) error {
	return structjson.UnmarshalStandard(raw, target, structjson.WithUnknownFieldPolicy(structjson.ErrorOnUnknown))
}

func (strictBodyDecoder) UnmarshalWithRepresentation(raw []byte, target any, metadata *shape.Representation) error {
	return structjson.UnmarshalStandard(raw, target, structjson.WithRepresentation(metadata), structjson.WithUnknownFieldPolicy(structjson.ErrorOnUnknown))
}
