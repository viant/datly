package spec

import "fmt"

// ValidateSequenceStrategy checks the component's optional native allocation
// selection. Empty delegates to the dialect; unsafe/read-only strategies are
// not part of the managed writer setting.
func (s *Settings) ValidateSequenceStrategy() error {
	if s == nil {
		return nil
	}
	switch s.SequenceStrategy {
	case "", "transient", "reservation":
		return nil
	}
	return fmt.Errorf("unsupported sequence_strategy %q: expected transient or reservation (omit for the native default)", s.SequenceStrategy)
}
