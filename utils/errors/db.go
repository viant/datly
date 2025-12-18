package errors

import (
	"errors"
	"strings"
)

// IsDatabaseError determines whether the supplied error was caused by the database or driver layer.
// We inspect the full error chain because many call-sites wrap driver errors with additional context.
func IsDatabaseError(err error) bool {
	if err == nil {
		return false
	}
	return hasDatabaseSignature(err)
}

func hasDatabaseSignature(err error) bool {
	for err != nil {
		if matchesDatabasePattern(err.Error()) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func matchesDatabasePattern(message string) bool {
	if message == "" {
		return false
	}
	lower := strings.ToLower(message)
	patterns := []string{
		"database error occured while fetching data",
		"database error occurred while fetching data",
		"error occured while connecting to database",
		"error occurred while connecting to database",
		"failed to get db",
		"failed to create stmt source",
		"too many connections",
		"connection refused",
		"driver: bad connection",
		"sql: transaction has already been committed or rolled back",
	}
	for _, pattern := range patterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
