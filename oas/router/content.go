package router

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/oas/coding"
	"github.com/viant/datly/oas/openapi3"
	"github.com/viant/datly/oas/spec"
)

type content struct {
	contentType string
	new         spec.Provider
	encoder     coding.Encoder
	decoder     coding.Decoder
}



func initContent(contentType string, mType *openapi3.MediaType) (*content, error) {
	switch contentType {
	case "application/json":
		schema := mType.Schema
		if schema == nil {
			return nil, fmt.Errorf("schema was empty")
		}
		provider, err := spec.LookupType(schema.Type)
		if err != nil {
			return nil, err
		}
		result := &content{
			new: provider,
		}
		a := result.new()
		if _, ok := a.(json.Marshaler);ok {

		}

		return result, nil
	}
	return nil, fmt.Errorf("unsupported content :%v", contentType)
}

