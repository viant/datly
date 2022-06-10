package marshal

type Transform struct {
	FieldName string
	Codec     string
}

type Transforms []*Transform

func (t Transforms) Index() map[string]*Transform {
	var result = map[string]*Transform{}
	for i, item := range t {
		result[item.FieldName] = t[i]
	}
	return result
}
