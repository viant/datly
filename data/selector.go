package data

//Selector allows customizing data fetched from Database
type (
	Selector struct {
		Columns      []string
		OrderBy      string
		Offset       int
		Limit        int
		Parameters   ParamState
		_columnNames map[string]bool
		Criteria     string
	}

	ParamState struct {
		Values interface{}
		Has    interface{}
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
