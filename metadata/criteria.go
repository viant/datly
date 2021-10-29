package metadata

//Criteria  represents FromFragments criteria
type Criteria struct {
	Expression string   `json:",omitempty"`
	Params     []string `json:",omitempty"`
}
