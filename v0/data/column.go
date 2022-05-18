package data

//Column represents view view column
type Column struct {
	Name       string `json:",omitempty"`
	DataType   string `json:",omitempty"`
	Expression string `json:",omitempty"`
}
