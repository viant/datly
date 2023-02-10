package config

import "github.com/viant/datly/plugins"

type Visitor struct {
	Ref      string
	name     string
	_visitor plugins.Valuer
}

func NewVisitor(name string, visitor plugins.Valuer) plugins.BasicCodec {
	return &Visitor{
		name:     name,
		_visitor: visitor,
	}
}

func (v *Visitor) Inherit(visitor plugins.BasicCodec) {
	v._visitor = visitor.Valuer()
}

func (v *Visitor) Valuer() plugins.Valuer {
	return v._visitor
}

func (v *Visitor) Name() string {
	return v.name
}

func (v *Visitor) Visitor() interface{} {
	return v._visitor
}
