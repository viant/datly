// Package access classifies failures returned by native MCP access callbacks.
package access

import (
	"errors"

	"github.com/viant/authz"
)

// Denied requires every cause to be a denial. A denial joined to a dependency
// failure cannot turn an incomplete authorization decision into an omission.
func Denied(err error) bool {
	if err == nil {
		return false
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		causes := wrapped.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !Denied(cause) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return Denied(wrapped.Unwrap())
	default:
		return errors.Is(err, authz.ErrDenied)
	}
}
