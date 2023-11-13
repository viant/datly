package state

import "strings"

// IsReservedAsyncState returns true if reserved async state
func IsReservedAsyncState(name string) bool {
	isAsync := false
	switch strings.ToLower(name) {
	case "userid", "useremail", "jobmatchkey":
		isAsync = true
	}
	return isAsync
}
