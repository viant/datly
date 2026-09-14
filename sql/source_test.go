package sql

import (
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/spec"
)

func TestResolveSource(t *testing.T) {
	tests := []struct {
		name    string
		source  *spec.ViewSource
		wantSQL string
		wantErr string
	}{
		{
			name: "URI", source: &spec.ViewSource{URI: "query.sql"},
			wantSQL: "SELECT id FROM users",
		},
		{
			name: "inline", source: &spec.ViewSource{
				SQL:    "SELECT * FROM (${embed:query.sql}) users",
				Embeds: []*spec.EmbeddedSQLRef{{Path: "query.sql", Raw: "${embed:query.sql}"}},
			},
			wantSQL: "SELECT * FROM (SELECT id FROM users) users",
		},
		{
			name: "missing token", source: &spec.ViewSource{
				SQL: "SELECT 1", Embeds: []*spec.EmbeddedSQLRef{{Path: "query.sql", Raw: "${embed:query.sql}"}},
			},
			wantErr: "does not contain resource token",
		},
	}
	resources := fstest.MapFS{"query.sql": {Data: []byte("SELECT id FROM users")}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ResolveSource("users", test.source, resources)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("ResolveSource() error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.source.SQL != test.wantSQL || len(test.source.Embeds) != 0 {
				t.Fatalf("source = %+v", test.source)
			}
		})
	}
}

func TestResolveSourcePreservesURIWhenInlineSQLIsAlreadyAvailable(t *testing.T) {
	source := &spec.ViewSource{SQL: "SELECT id FROM users", URI: "queries/users.sql"}
	if err := ResolveSource("users", source, nil); err != nil {
		t.Fatalf("ResolveSource() error = %v", err)
	}
	if source.SQL != "SELECT id FROM users" || source.URI != "queries/users.sql" || len(source.Embeds) != 0 {
		t.Fatalf("source = %+v", source)
	}
}
