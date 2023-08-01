package view

import "fmt"

// MatchStrategy in some cases it might be better to fetch parent View View and all Relation View in the same time
// and Merge it on the backend side in those cases ReadAll strategy will do that.
// in other cases it might be better to filter Relation View and fetch only those records that matches with View View
// in those cases ReadMatched will do that.
type MatchStrategy string

// Validate checks if MatchStrategy is valid
func (s MatchStrategy) Validate() error {
	switch s {
	case ReadAll, ReadMatched, ReadDerived:
		return nil
	}
	return fmt.Errorf("unsupported match strategy %v", s)
}

// SupportsParallel indicates whether MatchStrategy support parallel read.
func (s MatchStrategy) SupportsParallel() bool {
	return s == ReadAll
}

const (
	ReadAll     MatchStrategy = "read_all"     // read all and later we match on backend side
	ReadMatched MatchStrategy = "read_matched" // read parent View and then filter id to match with the current View
	ReadDerived MatchStrategy = "read_derived" // use parent sql selector to add criteria to the relation View, this can only work if the connector of the relation View and parent View is the same
)
