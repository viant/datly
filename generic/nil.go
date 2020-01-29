package generic

//nilValue is used to discriminate between unset fileds, and set filed with nil value (for patch operation)
var nilValue = make([]*interface{}, 1)[0]

//Value returns value
func Value(value interface{}) interface{} {
	if value == nilValue {
		return nil
	}
	return value
}
