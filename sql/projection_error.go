package sql

// UnresolvedProjectionError means static SQL inspection could not establish a
// projection. It does not prove a missing output or valid executable SQL.
// Registration may defer it; selected query construction must still report it.
type UnresolvedProjectionError struct {
	Cause error
}

func (e *UnresolvedProjectionError) Error() string {
	if e.Cause != nil {
		return "source projection is unresolved: " + e.Cause.Error()
	}
	return "source projection is unresolved"
}

func (e *UnresolvedProjectionError) Unwrap() error { return e.Cause }
