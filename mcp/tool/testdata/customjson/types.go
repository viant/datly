package customjson

import "encoding/json"

type Request struct {
	Lookup json.RawMessage `json:"lookup"`
	Nested *Options        `json:"options"`
}
type Options struct {
	Labels []string `json:"labels"`
	Next   *Options `json:"next"`
}

func (*Request) UnmarshalJSON([]byte) error { panic("discovery invoked custom decoder") }

type RequestWire Request
type OtherWire struct {
	Value string `json:"value"`
}
type CustomWire struct {
	Value string `json:"value"`
}

func (*CustomWire) UnmarshalJSON([]byte) error { panic("discovery invoked wire decoder") }

type Input struct {
	Body *Request `parameter:",kind=body" wireType:"github.com/viant/datly/mcp/tool/testdata/customjson.RequestWire" json:"request"`
}
