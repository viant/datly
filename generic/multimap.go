package generic

//Multimap represents generic  multi map
type Multimap struct {
	_proto *Proto
	_map   map[string][][]interface{}
	index  Index
}

//Proto returns multimap _proto
func (m *Multimap) Proto() *Proto {
	return m._proto
}

//First returns an element from multimap
func (s Multimap) First() *Object {
	if s.Size() == 0 {
		return nil
	}
	for _, v := range s._map {
		return &Object{_proto: s._proto, _data: v[0]}
	}
	return nil
}

//Add add item to a map
func (m *Multimap) Add(values map[string]interface{}) {
	object := &Object{_proto: m._proto, _data: make([]interface{}, 0)}
	object.Init(values)
	key := m.index(values)
	if _, ok := m._map[key]; !ok {
		m._map[key] = make([][]interface{}, 0)
	}
	m._map[key] = append(m._map[key], object._data)
}

func (m *Multimap) AddObject(object *Object) {
	key := m.index(object)
	if _, ok := m._map[key]; !ok {
		m._map[key] = make([][]interface{}, 0)
	}
	m._map[key] = append(m._map[key], object._data)
}

//Size return slice size
func (m Multimap) Size() int {
	return len(m._map)
}

//Range calls handler with every slice element
func (m Multimap) Range(handler func(item interface{}) (bool, error)) error {
	return m.Slices(func(key string, slice *Array) (b bool, err error) {
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
		slice := &Array{_proto: m._proto, _data: item}
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

//Slices iterate over object slice, any update to objects are applied to the slice
func (m *Multimap) Slices(handler func(key string, value *Array) (bool, error)) error {
	aMap := m._map
	for key, item := range aMap {
		slice := &Array{_proto: m._proto, _data: item}
		next, err := handler(key, slice)
		aMap[key] = slice._data
		if !next || err != nil {
			return err
		}
	}
	return nil
}

//Array returns a slice for specified key or nil
func (m *Multimap) Slice(key string) *Array {
	data, ok := m._map[key]
	if !ok {
		return nil
	}
	return &Array{_proto: m._proto, _data: data}
}

func (m Multimap) IsNil() bool {
	return len(m._map) == 0
}

func (m Multimap) Compact() *Compatcted {
	result := &Compatcted{Fields: m._proto.fields, Data: make([][]interface{}, 0)}
	for k := range m._map {
		result.Data = append(result.Data, m._map[k]...)
	}
	return result
}
