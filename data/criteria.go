package data

//Criteria  represents SQL criteria
type Criteria struct {
	Expression string `json:",omitempty"`
	Params []string `json:",omitempty"`
}
