package data

type Selector struct {
	Columns      []string
	OrderBy      string
	Offset       int
	Limit        int
	_columnNames map[string]bool
	Criteria     *Criteria
}

func (s *Selector) Init() {
	s._columnNames = Names(s.Columns).Index()
}
