package sql

import "fmt"

// UnknownProjectionColumnError identifies a requested selector field that is
// not in the authored projection. It carries no transport status or policy.
type UnknownProjectionColumnError struct{ Column string }

func (e *UnknownProjectionColumnError) Error() string {
	if e == nil {
		return "not found column"
	}
	return fmt.Sprintf("not found column %s", e.Column)
}
