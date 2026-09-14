package spec

// SelfReference describes how flat rows of one view form a tree.
type SelfReference struct {
	Holder string `json:"holder"`
	Child  string `json:"child"`
	Parent string `json:"parent"`
}
