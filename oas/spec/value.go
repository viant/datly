package spec

//Value represents input values from various locations
type Value struct {
	Location string
	Name     string
	Value    interface{}
	Matched  bool
	Valid    bool
}

//NewValue creates a value
func NewValue(loc, name string, value string) *Value {
	return &Value{
		Location: loc,
		Name:     name,
		Value:    value,
		Matched:  true,
	}
}
