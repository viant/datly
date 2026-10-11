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

func TestResolveSourceExpandsNestedResourcesRelativeToAuthoredFile(t *testing.T) {
	resources := fstest.MapFS{
		"creative/query.sql":      {Data: []byte("SELECT c.id FROM records c JOIN (${embed:ids.sql}) ids ON ids.id=c.id")},
		"creative/ids.sql":        {Data: []byte("SELECT id FROM (${embed:detail/ids.sql}) detail")},
		"creative/detail/ids.sql": {Data: []byte("SELECT id FROM records")},
	}
	want := "SELECT c.id FROM records c JOIN (SELECT id FROM (SELECT id FROM records) detail) ids ON ids.id=c.id"
	for _, source := range []*spec.ViewSource{
		{URI: "creative/query.sql"},
		{URI: "creative/query.sql", Embeds: []*spec.EmbeddedSQLRef{{Path: "creative/ids.sql", Raw: "${embed:ids.sql}"}}},
		{SQL: "${embed:creative/query.sql}", Embeds: []*spec.EmbeddedSQLRef{{Path: "creative/query.sql", Raw: "${embed:creative/query.sql}"}}},
	} {
		if err := ResolveSource("creative", source, resources); err != nil {
			t.Fatal(err)
		}
		if source.SQL != want || len(source.Embeds) != 0 {
			t.Fatalf("nested authored resources unresolved: %+v", source)
		}
	}
}

func TestResolveSourceNestedFailuresDoNotChangeAuthoredSource(t *testing.T) {
	for _, body := range []string{"SELECT * FROM (${embed:missing.sql}) ids", "SELECT * FROM (${embed:query.sql}) ids"} {
		resources := fstest.MapFS{"creative/query.sql": {Data: []byte(body)}}
		source := &spec.ViewSource{SQL: "${embed:creative/query.sql}", Embeds: []*spec.EmbeddedSQLRef{{Path: "creative/query.sql", Raw: "${embed:creative/query.sql}"}}}
		if err := ResolveSource("creative", source, resources); err == nil {
			t.Fatalf("nested invalid source accepted: %s", body)
		}
		if source.SQL != "${embed:creative/query.sql}" || len(source.Embeds) != 1 {
			t.Fatal("failure changed source authority")
		}
	}
}
