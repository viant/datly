package data

//Criteria  represents FromFragments criteria
type Criteria struct {
	Expression string   `json:",omitempty"`
	Parameters []string `json:",omitempty"`
}
