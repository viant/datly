package generic

import (
	"encoding/json"
	"github.com/viant/toolbox"
)

//Object represents dynamic object
type Object struct {
	_proto *Proto
	_data  []interface{}
}

//Proto returns object _proto
func (o *Object) Proto() *Proto {
	return o._proto
}

//Init initialise entire object
func (o *Object) Init(values map[string]interface{}) {
	o._data = o._proto.asValues(values)
}

//AsMap return map
func (o *Object) AsMap() map[string]interface{} {
	return o._proto.asMap(o._data)
}

//SetValue sets values
func (o *Object) SetValue(name string, value interface{}) {
	field := o._proto.getField(name, value)
	field.Set(value, &o._data)
}

//Value get value for supplied name
func (o *Object) Value(name string) interface{} {
	field := o._proto.Field(name)
	if field == nil {
		return nil
	}
	return field.Get(o._data)
}

//FloatValue return float for supplied name
func (o *Object) FloatValue(name string) (*float64, error) {
	val := o.Value(name)
	if val == nil {
		return nil, nil
	}
	casted, err := toolbox.ToFloat(val)
	return &casted, err
}

//IntValue returns int value
func (o *Object) IntValue(name string) (*int, error) {
	val := o.Value(name)
	if val == nil {
		return nil, nil
	}
	casted, err := toolbox.ToInt(val)
	return &casted, err
}

//IntValue returns int value
func (o *Object) StringValue(name string) *string {
	val := o.Value(name)
	if val == nil {
		return nil
	}
	casted := toolbox.AsString(val)
	return &casted
}

//MarshalJSON converts object to JSON object
func (o Object) MarshalJSON() ([]byte, error) {
	aMap := o._proto.asMap(o._data)
	return json.Marshal(aMap)
}
