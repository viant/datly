package sql

import (
	"strings"
	"testing"
)

func TestReplaceTable(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		source     string
		target     string
		wantParts  []string
		avoidParts []string
		wantErr    bool
	}{
		{
			name:       "root table only",
			sql:        "SELECT events.id, 'events' AS label FROM events",
			source:     "events",
			target:     "events_2026",
			wantParts:  []string{"FROM events_2026", "'events' AS label"},
			avoidParts: []string{"FROM events "},
		},
		{
			name:   "cte and union branches",
			sql:    "WITH base AS (SELECT id FROM events) SELECT id FROM events UNION ALL SELECT id FROM events",
			source: "events",
			target: "events_2026",
			wantParts: []string{
				"WITH base AS (SELECT id FROM events_2026)",
				"SELECT id FROM events_2026 UNION ALL SELECT id FROM events_2026",
			},
		},
		{
			name:      "qualified table",
			sql:       "SELECT id FROM analytics.events e",
			source:    "analytics.events",
			target:    "analytics.events_2026",
			wantParts: []string{"FROM analytics.events_2026 e"},
		},
		{
			name:    "source absent",
			sql:     "SELECT id FROM users",
			source:  "events",
			target:  "events_2026",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := ReplaceTable(test.sql, test.source, test.target)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected error, got SQL %s", actual)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReplaceTable failed: %v", err)
			}
			for _, part := range test.wantParts {
				if !strings.Contains(actual, part) {
					t.Fatalf("expected %q in %s", part, actual)
				}
			}
			for _, part := range test.avoidParts {
				if strings.Contains(actual, part) {
					t.Fatalf("did not expect %q in %s", part, actual)
				}
			}
		})
	}
}
