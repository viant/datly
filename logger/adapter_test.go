package logger

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/requesttrace"
)

func TestReqTraceID(t *testing.T) {
	require.Equal(t, "unknown", reqTraceID(nil))
	require.Equal(t, "unknown", reqTraceID(context.Background()))

	ctx := requesttrace.Ensure(context.Background(), "trace-123")
	require.Equal(t, "trace-123", reqTraceID(ctx))
}

func TestNormalizeDatabaseError(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil",
			err:      nil,
			expected: "",
		},
		{
			name:     "bigquery due to",
			err:      errors.New("failed to run query: SELECT * FROM table, due to googleapi: Error 400: invalidQuery"),
			expected: "googleapi: Error 400: invalidQuery",
		},
		{
			name:     "sqlx wrapped query",
			err:      errors.New("database error occured while fetching Data for view v failed to run query: SELECT * FROM table WHERE id = ?"),
			expected: "database error occured while fetching Data for view v",
		},
		{
			name:     "raw failed query",
			err:      errors.New("failed to run query: SELECT * FROM table WHERE id = ?"),
			expected: "failed to run query",
		},
		{
			name:     "plain error",
			err:      errors.New("connection refused"),
			expected: "connection refused",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.expected, normalizeDatabaseError(testCase.err))
		})
	}
}
