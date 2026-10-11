package spec

import "fmt"

// ValidateComponentCallPolicy accepts buffered calls by default, an explicit
// imperative override, and the separate independent-child transaction mode.
func (s *Settings) ValidateComponentCallPolicy() error {
	if s == nil {
		return nil
	}
	switch s.ComponentCallPolicy {
	case "", "imperative", "buffered":
	default:
		return fmt.Errorf("unsupported component call policy %q", s.ComponentCallPolicy)
	}
	if s.ComponentCallPolicy == "buffered" && s.IndependentChildTransactions {
		return fmt.Errorf("buffered component calls cannot use independent child transactions")
	}
	return nil
}
