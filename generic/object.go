package generic

import (
	"encoding/json"
)

//Object represents dynamic object
type Object struct {
	proto *Proto
	_data []interface{}
}

///Init initialise entire object
func (o *Object) Init(values map[string]interface{}) {
	o._data = o.proto.asValues(values)
}

//AsMap return map
func (o *Object) AsMap() map[string]interface{} {
	return o.proto.asMap(o._data)
}

//SetValue sets values
func (o *Object) SetValue(name string, value interface{}) {
	field := o.proto.getField(name, value)
	field.Set(value, &o._data)
}

//GetValue get values
func (o *Object) GetValue(name string) interface{} {
	field := o.proto.Field(name)
	if field == nil {
		return nil
	}
	return field.Get(o._data)
}

//MarshalJSON converts object to JSON object
func (d Object) MarshalJSON() ([]byte, error) {
	aMap := d.proto.asMap(d._data)
	return json.Marshal(aMap)
}
