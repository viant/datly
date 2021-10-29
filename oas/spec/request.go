package spec


//Request represent generic request
type Request struct {
	ContentType string
	Values   []*Value
	Body interface{}
}
