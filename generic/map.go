package generic

//Map represents generic map
type Map struct {
	proto *Proto
	_map  map[string][]interface{}
	index Index
}

//Range call handler with every slice element
func (s Map) Range(handler func(item interface{}) (bool, error)) error {
	return s.Objects(func(item *Object) (b bool, err error) {
		return handler(item.AsMap())
	})
}



//Add add item to a map
func (m *Map) Add(values map[string]interface{}) {
	object := &Object{proto: m.proto, _data: make([]interface{}, 0)}
	object.Init(values)
	key := m.index(values)
	m._map[key] = object._data
}

//Size return slice size
func (m Map) Size() int {
	return len(m._map)
}


//Pairs iterate over object slice, any update to objects are applied to the slice
func (m *Map) Pairs(handler func(key string, item *Object) (bool, error)) error {
	aMap := m._map
	object := &Object{proto: m.proto}
	for key, item := range aMap {
		object._data = item
		next, err := handler(key, object)
		aMap[key] = object._data
		if !next || err != nil {
			return err
		}
	}
	return nil
}

//Objects iterate over object slice, any update to objects are applied to the slice
func (m *Map) Objects(handler func(item *Object) (bool, error)) error {
	aMap := m._map
	object := &Object{proto: m.proto}
	for key, item := range aMap {
		object._data = item
		next, err := handler(object)
		aMap[key] = object._data
		if !next || err != nil {
			return err
		}
	}
	return nil
}



//Object returns an object for specified key or nil
func (m *Map) Object(key string) *Object {
	data, ok := m._map[key]
	if ! ok {
		return nil
	}
	return &Object{proto: m.proto, _data: data}
}

