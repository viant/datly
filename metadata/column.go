package metadata

//Column represents data view column
type Column struct {
	Name       string `json:",omitempty"`
	DataType   string `json:",omitempty"`
	Expression string `json:",omitempty"`
	FieldIndex int    `json:",omitempty"`
	Tag        string `json:",omitempty"`
}


//Columns type columns
type Columns []Column

func (c Columns) Index() map[string]*Column {
	var result = make(map[string]*Column)
	if len(c) == 0 {
		return result
	}
	for i, col := range c {
		result[col.Name] = &c[i]
	}
	return result
}
