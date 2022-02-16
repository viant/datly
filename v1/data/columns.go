package data

//Columns wrap slice of Column
type Columns []*Column

//Index indexes columns by Column Name using various strategies.
func (c Columns) Index() map[string]*Column {
	result := make(map[string]*Column)
	for i, column := range c {
		keys := KeysOf(column.Name, true)
		for _, key := range keys {
			result[key] = c[i]
		}
	}
	return result
}

//Init initializes every Column in the slice.
func (c Columns) Init() error {
	for i := range c {
		if err := c[i].Init(); err != nil {
			return nil
		}
	}

	return nil
}
