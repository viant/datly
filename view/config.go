package view

//Config represent a view selector for projection and selection
type Config struct {
	//TODO: Should order by be a slice?
	OrderBy string `json:",omitempty"`
	Limit   int    `json:",omitempty"`
}
