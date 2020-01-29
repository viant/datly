package data

//Visitor represents a visitor
type Visitor struct {
	Visitor string
	_visit  Visit
}

//Init initialises visitors
func (v *Visitor) Init() error {
	var err error
	v._visit, err = VisitorRegistry().Get(v.Visitor)
	return err
}

//Visit visit an object
func (v *Visitor) Visit(ctx *Context, value *Value) (bool, error) {
	return v._visit(ctx, value)
}
