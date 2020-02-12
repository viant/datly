package generic

//Map represents generic map
type Map struct {
	_proto *Proto
	_map   map[string][]interface{}
	index  Index
}

//Proto returns map _proto
func (m *Map) Proto() *Proto {
	return m._proto
}

//First return a  map elements
func (s Map) First() *Object {
	if s.Size() == 0 {
		return nil
	}
	for _, v := range s._map {
		return &Object{_proto: s._proto, _data: v}
	}
	return nil
}

//Range calls handler with every slice element
func (m Map) Range(handler func(item interface{}) (bool, error)) error {
	return m.Objects(func(item *Object) (b bool, err error) {
		return handler(item.AsMap())
	})
}

//Add add item to a map
func (m *Map) Add(values map[string]interface{}) {
	object := &Object{_proto: m._proto, _data: make([]interface{}, 0)}
	object.Init(values)
	key := m.index(values)
	m._map[key] = object._data
}

func (m *Map) AddObject(object *Object) {
	key := m.index(object)
	m._map[key] = object._data
}

//Size return slice size
func (m Map) Size() int {
	return len(m._map)
}

//Pairs iterate over object slice, any update to objects are applied to the slice
func (m *Map) Pairs(handler func(key string, item *Object) (bool, error)) error {
	aMap := m._map
	object := &Object{_proto: m._proto}
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
	object := &Object{_proto: m._proto}
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
	if !ok {
		return nil
	}
	return &Object{_proto: m._proto, _data: data}
}

func (m Map) Compact() *Compatcted {
	result := &Compatcted{Fields: m._proto.fields, Data: make([][]interface{}, 0)}
	for k := range m._map {
		result.Data = append(result.Data, m._map[k])
	}
	return result
}
