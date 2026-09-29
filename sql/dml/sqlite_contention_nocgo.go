//go:build !cgo

package dml

// The mattn SQLite driver cannot execute without CGO. Other drivers retain
// SQLState/MySQL contention handling in mutationContention.
func sqliteMutationContention(error) bool { return false }
