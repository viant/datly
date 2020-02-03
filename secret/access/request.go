package access

//Request represents secure request
type Request struct {
	Method    string `json:",omitempty"`
	Parameter string `json:",omitempty"`
	URL       string `json:",omitempty"`
	Key       string `json:",omitempty"`
}
