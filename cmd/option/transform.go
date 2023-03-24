package option

type TransformOption struct {
	TransformKind string `json:",omitempty" yaml:",omitempty"`
	Codec         string `json:",omitempty" yaml:",omitempty"`
	Transformer   string `json:",omitempty" yaml:",omitempty"`
}
