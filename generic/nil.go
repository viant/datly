package generic

//Nilable represent a type that can be nil
type Nilable interface {
	IsNil() bool
}

//Zeroable represent uninitialise type
type Zeroable interface {
	IsZero() bool
}

//Value returns value
func Value(value interface{}) interface{} {
	if value == NilValue {
		return nil
	}
	return value
}
