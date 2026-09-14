package dql

import "errors"

type sourceError struct {
	offset int
	end    int
	err    error
}

func (e *sourceError) Error() string { return e.err.Error() }

func (e *sourceError) Unwrap() error { return e.err }

func wrapDirectiveError(err error, block directiveBlock) error {
	if err == nil || block.end <= block.start {
		return err
	}
	var existing *sourceError
	if errors.As(err, &existing) {
		return err
	}
	return &sourceError{offset: block.start, end: block.end, err: err}
}

// DiagnosticForError extracts an authored-source diagnostic from a DQL parse
// error when the focused parser knows the failing directive span.
func DiagnosticForError(err error) (SourceDiagnostic, bool) {
	var sourceErr *sourceError
	if !errors.As(err, &sourceErr) {
		return SourceDiagnostic{}, false
	}
	return SourceDiagnostic{
		Code:    DiagnosticParse,
		Message: sourceErr.Error(),
		Offset:  sourceErr.offset,
		End:     sourceErr.end,
	}, true
}

func earliestDirectiveError(errs ...error) error {
	var result error
	resultOffset := int(^uint(0) >> 1)
	for _, err := range errs {
		if err == nil {
			continue
		}
		var sourceErr *sourceError
		if !errors.As(err, &sourceErr) {
			if result == nil {
				result = err
			}
			continue
		}
		if result == nil || sourceErr.offset < resultOffset {
			result = err
			resultOffset = sourceErr.offset
		}
	}
	return result
}
