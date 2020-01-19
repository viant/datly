package generic

import (
	"bytes"
	"encoding/json"
)

//Slice represents dynamic object slice
type Slice struct {
	proto *Proto
	_data [][]interface{}
}

//Add add elements to a slice
func (s *Slice) Add(aMap map[string]interface{}) {
	values := s.proto.asValues(aMap)
	data := s._data
	data = append(data, values)
	s._data = data
}

//Size return slice size
func (s Slice) Size() int {
	return len(s._data)
}

//Objects iterate over object slice, any update to objects are applied to the slice
func (s *Slice) Objects(handler func(item *Object) (bool, error)) error {
	data := s._data
	object := &Object{proto: s.proto}
	for i, item := range data {
		object._data = item
		next, err := handler(object)
		data[i] = object._data
		if !next || err != nil {
			return err
		}
	}
	return nil
}

//Range call handler with every slice element
func (s Slice) Range(handler func(item interface{}) (bool, error)) error {
	return s.Objects(func(item *Object) (b bool, err error) {
		return handler(item.AsMap())
	})
}

//MarshalJSON converts slice item to JSON array.
func (d Slice) MarshalJSON() ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := buf.Write([]byte("["))
	if err != nil {
		return nil, err
	}
	i := 0
	if err = d.Objects(func(object *Object) (b bool, err error) {
		if i > 0 {
			_, err := buf.Write([]byte(","))
			if err != nil {
				return false, err
			}
		}
		i++
		data, err :=json.Marshal(object)
		if err != nil {
			return false, err
		}
		_, err = buf.Write(data)
		return err == nil, err
	});err != nil {
		return nil, err
	}
	if _, err := buf.Write([]byte("]")); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}



