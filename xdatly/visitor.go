package xdatly

type Visitor struct {
	Ref      string
	name     string
	_visitor Valuer
}

func NewVisitor(name string, visitor Valuer) BasicCodec {
	return &Visitor{
		name:     name,
		_visitor: visitor,
	}
}

func (v *Visitor) Inherit(visitor BasicCodec) {
	v._visitor = visitor.Valuer()
}

func (v *Visitor) Valuer() Valuer {
	return v._visitor
}

func (v *Visitor) Name() string {
	return v.name
}

func (v *Visitor) Visitor() interface{} {
	return v._visitor
}
