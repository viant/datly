package writer

import "errors"

// ErrDeleteNotFound reports a strict delete whose complete identity did not
// match a current row. Callers can preserve their not-found contract with
// errors.Is without matching the handler's diagnostic text.
var ErrDeleteNotFound = errors.New("delete requires a matched complete identity")
