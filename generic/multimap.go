package generic

//Multimap represents generic  multi map
type Multimap struct {
	proto *Proto
	_map  map[string][][]interface{}
	index Index
}

//Add add item to a map
func (m *Multimap) Add(values map[string]interface{}) {
	object := &Object{proto: m.proto, _data: make([]interface{}, 0)}
	object.Init(values)
	key := m.index(values)
	if _, ok := m._map[key]; !ok {
		m._map[key] = make([][]interface{}, 0)
	}
	m._map[key] = append(m._map[key], object._data)
}

//Size return slice size
func (m Multimap) Size() int {
	return len(m._map)
}

//Range call handler with every slice element
func (s Multimap) Range(handler func(item interface{}) (bool, error)) error {
	return s.Slices(func(key string, slice *Slice) (b bool, err error) {
		cont := false
		err = slice.Objects(func(item *Object) (b bool, err error) {
			cont, err = handler(item.AsMap())
			return cont, err
		})
		return cont && err != nil, err
	})
}

//Objects call handler for every object in this collection
func (m *Multimap) Objects(handler func(item *Object) (bool, error)) error {
	aMap := m._map
	next := false
	var err error
	for key, item := range aMap {
		slice := &Slice{proto: m.proto, _data: item}
		err = slice.Objects(func(item *Object) (b bool, err error) {
			next, err = handler(item)
			return next, err
		})
		aMap[key] = slice._data
		if !next || err != nil {
			break
		}
	}
	return err
}

//Objects iterate over object slice, any update to objects are applied to the slice
func (m *Multimap) Slices(handler func(key string, value *Slice) (bool, error)) error {
	aMap := m._map
	for key, item := range aMap {
		slice := &Slice{proto: m.proto, _data: item}
		next, err := handler(key, slice)
		aMap[key] = slice._data
		if !next || err != nil {
			return err
		}
	}
	return nil
}

//Object returns a slice for specified key or nil
func (m *Multimap) Slice(key string) *Slice {
	data, ok := m._map[key]
	if ! ok {
		return nil
	}
	return &Slice{proto: m.proto, _data: data}
}
