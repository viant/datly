package generic

//Array represents dynamic object slice
type Array struct {
	_proto *Proto
	_data  [][]interface{}
}

//Proto returns slice _proto
func (s *Array) Proto() *Proto {
	return s._proto
}

//Add add elements to a slice
func (s *Array) Add(aMap map[string]interface{}) {
	values := s._proto.asValues(aMap)
	data := s._data
	data = append(data, values)
	s._data = data
}

func (s *Array) AddObject(object *Object) {
	data := s._data
	data = append(data, object._data)
	s._data = data
}

//Size return slice size
func (s Array) Size() int {
	return len(s._data)
}

//Objects iterate over object slice, any update to objects are applied to the slice
func (s Array) First() *Object {
	if s.Size() == 0 {
		return nil
	}
	return &Object{_proto: s._proto, _data: s._data[0]}
}

//Objects iterate over object slice, any update to objects are applied to the slice
func (s *Array) Objects(handler func(item *Object) (bool, error)) error {
	data := s._data
	object := &Object{_proto: s._proto}
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
func (s Array) Range(handler func(item interface{}) (bool, error)) error {
	return s.Objects(func(item *Object) (b bool, err error) {
		return handler(item.AsMap())
	})
}

func (s Array) Compact() *Compatcted {
	result := &Compatcted{Fields: s._proto.fields, Data: s._data}
	return result
}
