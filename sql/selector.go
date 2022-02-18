package sql

type Selector struct {
	Name string
}

func (s *Selector) Validate(_ map[string]Kind) error {
	return nil
}
