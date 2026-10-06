package spec

import "fmt"

// ValidateComponentCallPolicy preserves ordinary imperative calls by default.
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
