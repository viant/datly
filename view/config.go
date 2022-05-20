package view

//Config represent a view config selector
type Config struct {
	//TODO: Should order by be a slice?
	OrderBy     string `json:",omitempty"`
	Limit       int    `json:",omitempty"`
	Constraints *Constraints
}
