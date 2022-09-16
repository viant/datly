package marshal

type Transform struct {
	Path  string
	Codec string
}

type Transforms []*Transform
type TransformIndex map[string]Transforms

func (t Transforms) Index() map[string]*Transform {
	var result = map[string]*Transform{}
	for i, item := range t {
		result[item.Path] = t[i]
	}
	return result
}
