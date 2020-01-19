package data

//Join represent a join
type Join struct {
	Type  string `json:",omitempty"`
	Alias string `json:",omitempty"`
	Table string `json:",omitempty"`
	On    string `json:",omitempty"`
}

