package view

//Selector allows customizing view fetched from Database
type (
	Selector struct {
		Columns      []string   `json:",omitempty"`
		Fields       []string   `json:",omitempty"`
		OrderBy      string     `json:",omitempty"`
		Offset       int        `json:",omitempty"`
		Limit        int        `json:",omitempty"`
		Parameters   ParamState `json:",omitempty"`
		_columnNames map[string]bool
		Criteria     string `json:",omitempty"`
	}

	ParamState struct {
		Values interface{} `json:",omitempty"`
		Has    interface{} `json:",omitempty"`
	}
)

//Init initializes Selector
func (s *Selector) Init() {
	s._columnNames = Names(s.Columns).Index()
}

//Has checks if Field is present in Selector.Columns
func (s *Selector) Has(field string) bool {
	_, ok := s._columnNames[field]
	return ok
}
