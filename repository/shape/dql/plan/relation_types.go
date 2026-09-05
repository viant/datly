package plan

type relationLink struct {
	Line      int
	Column    string
	Namespace string
}

type relationMeta struct {
	Line      int
	Name      string
	Holder    string
	Ref       string
	On        []relationLink
	OfOn      []relationLink
	PairCount int
}

type projectionMeta struct {
	Columns map[string]bool
	HasStar bool
}

type viewMeta struct {
	Name       string
	Line       int
	HasSQL     bool
	Aliases    map[string]bool
	Namespaces map[string]bool
	Projection projectionMeta
	Relations  []relationMeta
}
