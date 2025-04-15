package contract

type Path struct {
	URI         string `json:",omitempty" yaml:"URI"`
	Method      string `json:",omitempty" yaml:"Method"`
	Description string `json:",omitempty" yaml:"Description"` // optional description for documentation purposes
	key         string
}

func (r *Path) Equals(candidate *Path) bool {
	if r.URI != candidate.URI {
		return false
	}
	if r.Method != candidate.Method {
		return false
	}
	return true
}
func (r *Path) Key() string {
	if r.key != "" {
		return r.key
	}
	r.key = r.Method + ":" + r.URI
	return r.key
}

func (r *Path) HttpMethod() string {
	return r.Method
}

func NewPath(method, uri string) *Path {
	return &Path{Method: method, URI: uri}
}
