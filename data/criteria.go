package data

//Criteria  represents WhereFragment criteria
type Criteria struct {
	Expression string `json:",omitempty"`
	Sanitize   *bool
	_sanitize  bool
}
