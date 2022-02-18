package sql

type Selector struct {
	Name string
}

func (s *Selector) Validate(allowed map[string]int) error {
	return nil
}
