package component

type Path struct {
	URI    string `json:",omitempty"`
	Method string `json:",omitempty"`
}

func (r *Path) HttpMethod() string {
	return r.Method
}
