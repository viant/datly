package indexfields

type Names []string
type Counts map[string]int
type Label string
type Alias = Names

type Row struct {
	ID      int
	Names   Names
	Counts  Counts
	Alias   Alias
	Label   Label
	Dynamic any
}
