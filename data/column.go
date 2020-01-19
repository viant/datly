package data

//Column represents data view column
type Column struct {
	Name string `json:",omitempty"`
	DataType string `json:",omitempty"`
	Expression string `json:",omitempty"`
}
