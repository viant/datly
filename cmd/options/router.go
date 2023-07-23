package options

type Router struct {
	RouterURL string `json:",omitempty" yaml:",omitempty"`
	URL       string `json:",omitempty" yaml:",omitempty"`
	Routes    []struct {
		SourceURL string
	}
}
